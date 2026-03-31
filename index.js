import 'dotenv/config';
import express from 'express';
import pg from 'pg';
import path from 'path';
import { fileURLToPath } from 'url';

const { Pool } = pg;
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3000;

// DB connection — use SSL for RDS, set search_path to linear_sync schema
const pgConfig = { connectionString: process.env.DATABASE_URL };
if (process.env.DATABASE_URL?.includes('rds.amazonaws.com')) {
  pgConfig.ssl = { rejectUnauthorized: false };
}
const pool = new Pool(pgConfig);

// Init schema — create linear_sync schema + tables within it
async function initDB() {
  await pool.query(`CREATE SCHEMA IF NOT EXISTS linear_sync`);
  await pool.query(`SET search_path TO linear_sync`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS linear_sync.linear_projects (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      color TEXT,
      state TEXT,
      url TEXT,
      created_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ,
      synced_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS linear_sync.linear_issues (
      id TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      description TEXT,
      url TEXT,
      priority INT,
      state_name TEXT,
      state_type TEXT,
      state_color TEXT,
      assignee_name TEXT,
      assignee_email TEXT,
      team_name TEXT,
      team_key TEXT,
      project_id TEXT,
      labels JSONB DEFAULT '[]',
      created_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ,
      synced_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS linear_sync.sync_log (
      id SERIAL PRIMARY KEY,
      started_at TIMESTAMPTZ DEFAULT NOW(),
      completed_at TIMESTAMPTZ,
      issues_synced INT DEFAULT 0,
      projects_synced INT DEFAULT 0,
      status TEXT DEFAULT 'running',
      error TEXT
    );
  `);
  console.log('✅ DB schema ready');
}

// Linear GraphQL fetch
async function linearQuery(query, variables = {}) {
  const res = await fetch('https://api.linear.app/graphql', {
    method: 'POST',
    headers: {
      'Authorization': process.env.LINEAR_API_KEY,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query, variables }),
  });
  const json = await res.json();
  if (json.errors) throw new Error(json.errors.map(e => e.message).join(', '));
  return json.data;
}

// Fetch all pages of a query
async function fetchAllPages(query, dataKey) {
  const results = [];
  let after = null;
  let hasMore = true;
  while (hasMore) {
    const data = await linearQuery(query, { after });
    const page = data[dataKey];
    results.push(...page.nodes);
    hasMore = page.pageInfo.hasNextPage;
    after = page.pageInfo.endCursor;
  }
  return results;
}

const PROJECTS_QUERY = `
  query Projects($after: String) {
    projects(first: 50, after: $after) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id name description color state url createdAt updatedAt
        teams { nodes { id name } }
      }
    }
  }
`;

const ISSUES_QUERY = `
  query Issues($after: String) {
    issues(first: 50, after: $after) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id title description url priority createdAt updatedAt
        state { id name type color }
        assignee { id name email }
        team { id name key }
        project { id name color }
        labels { nodes { id name color } }
      }
    }
  }
`;

// Active sync jobs
const syncJobs = {};

async function runSync(syncId) {
  const client = await pool.connect();
  try {
    await client.query('SET search_path TO linear_sync');
    syncJobs[syncId] = { status: 'running', progress: 'Fetching projects...' };

    // Fetch projects
    const projects = await fetchAllPages(PROJECTS_QUERY, 'projects');
    syncJobs[syncId].progress = `Syncing ${projects.length} projects...`;

    for (const p of projects) {
      await client.query(`
        INSERT INTO linear_sync.linear_projects (id, name, description, color, state, url, created_at, updated_at, synced_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
        ON CONFLICT (id) DO UPDATE SET
          name=EXCLUDED.name, description=EXCLUDED.description, color=EXCLUDED.color,
          state=EXCLUDED.state, url=EXCLUDED.url, updated_at=EXCLUDED.updated_at, synced_at=NOW()
      `, [p.id, p.name, p.description, p.color, p.state, p.url, p.createdAt, p.updatedAt]);
    }

    // Update sync log
    await client.query(`UPDATE linear_sync.sync_log SET projects_synced=$1 WHERE id=$2`, [projects.length, syncId]);

    // Fetch issues
    syncJobs[syncId].progress = 'Fetching issues...';
    const issues = await fetchAllPages(ISSUES_QUERY, 'issues');
    syncJobs[syncId].progress = `Syncing ${issues.length} issues...`;

    for (const issue of issues) {
      await client.query(`
        INSERT INTO linear_sync.linear_issues (id, title, description, url, priority, state_name, state_type, state_color,
          assignee_name, assignee_email, team_name, team_key, project_id, labels, created_at, updated_at, synced_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW())
        ON CONFLICT (id) DO UPDATE SET
          title=EXCLUDED.title, description=EXCLUDED.description, url=EXCLUDED.url,
          priority=EXCLUDED.priority, state_name=EXCLUDED.state_name, state_type=EXCLUDED.state_type,
          state_color=EXCLUDED.state_color, assignee_name=EXCLUDED.assignee_name,
          assignee_email=EXCLUDED.assignee_email, team_name=EXCLUDED.team_name,
          team_key=EXCLUDED.team_key, project_id=EXCLUDED.project_id, labels=EXCLUDED.labels,
          updated_at=EXCLUDED.updated_at, synced_at=NOW()
      `, [
        issue.id, issue.title, issue.description, issue.url, issue.priority,
        issue.state?.name, issue.state?.type, issue.state?.color,
        issue.assignee?.name, issue.assignee?.email,
        issue.team?.name, issue.team?.key,
        issue.project?.id,
        JSON.stringify((issue.labels?.nodes || []).map(l => ({ id: l.id, name: l.name, color: l.color }))),
        issue.createdAt, issue.updatedAt
      ]);
    }

    // Complete
    await client.query(`
      UPDATE linear_sync.sync_log SET completed_at=NOW(), issues_synced=$1, projects_synced=$2, status='completed'
      WHERE id=$3
    `, [issues.length, projects.length, syncId]);

    syncJobs[syncId] = { status: 'completed', issues: issues.length, projects: projects.length };
  } catch (err) {
    console.error('Sync error:', err);
    try {
      await client.query(`UPDATE linear_sync.sync_log SET completed_at=NOW(), status='error', error=$1 WHERE id=$2`, [err.message, syncId]);
    } catch {}
    syncJobs[syncId] = { status: 'error', error: err.message };
  } finally {
    client.release();
  }
}

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// API Routes
app.get('/api/stats', async (req, res) => {
  try {
    const [issuesRes, projectsRes, lastSync] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM linear_sync.linear_issues'),
      pool.query('SELECT COUNT(*) FROM linear_sync.linear_projects'),
      pool.query(`SELECT * FROM linear_sync.sync_log WHERE status='completed' ORDER BY completed_at DESC LIMIT 1`),
    ]);
    res.json({
      totalIssues: parseInt(issuesRes.rows[0].count),
      totalProjects: parseInt(projectsRes.rows[0].count),
      lastSync: lastSync.rows[0] || null,
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/issues', async (req, res) => {
  try {
    const { search, project, team, page = 1 } = req.query;
    const limit = 50;
    const offset = (page - 1) * limit;
    let where = 'WHERE 1=1';
    const params = [];
    if (search) { params.push(`%${search}%`); where += ` AND (i.title ILIKE $${params.length} OR i.team_name ILIKE $${params.length})`; }
    if (project) { params.push(project); where += ` AND i.project_id = $${params.length}`; }
    if (team) { params.push(team); where += ` AND i.team_name = $${params.length}`; }
    params.push(limit, offset);
    const result = await pool.query(`
      SELECT i.*, p.name as project_name, p.color as project_color
      FROM linear_sync.linear_issues i
      LEFT JOIN linear_sync.linear_projects p ON i.project_id = p.id
      ${where}
      ORDER BY i.updated_at DESC
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `, params);
    const countResult = await pool.query(`SELECT COUNT(*) FROM linear_sync.linear_issues i ${where}`, params.slice(0, -2));
    res.json({ issues: result.rows, total: parseInt(countResult.rows[0].count), page: parseInt(page), limit });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/projects', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT p.*, COUNT(i.id) as issue_count
      FROM linear_sync.linear_projects p
      LEFT JOIN linear_sync.linear_issues i ON i.project_id = p.id
      GROUP BY p.id
      ORDER BY p.updated_at DESC
    `);
    res.json({ projects: result.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sync-log', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM linear_sync.sync_log ORDER BY started_at DESC LIMIT 10');
    res.json({ logs: result.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/sync', async (req, res) => {
  try {
    const result = await pool.query(`INSERT INTO linear_sync.sync_log (status) VALUES ('running') RETURNING id`);
    const syncId = result.rows[0].id;
    runSync(syncId); // async, don't await
    res.json({ syncId, status: 'started' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sync/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (syncJobs[id]) return res.json(syncJobs[id]);
    const result = await pool.query('SELECT * FROM linear_sync.sync_log WHERE id=$1', [id]);
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Health check
app.get('/health', (req, res) => res.json({ status: 'ok' }));

// Start
initDB().then(() => {
  app.listen(PORT, () => console.log(`🚀 Linear Sync running on http://localhost:${PORT}`));
}).catch(err => {
  console.error('Failed to init DB:', err);
  process.exit(1);
});
