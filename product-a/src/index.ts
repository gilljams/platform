import express from "express";
import cors from "cors";
import { Kafka, Partitioners } from "kafkajs";
import Database from "better-sqlite3";
import { v4 as uuid } from "uuid";
import path from "path";
import fs from "fs";

// ── Database ──

const DB_PATH = path.join(__dirname, "..", "data", "product-a.db");
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

db.exec(`
  CREATE TABLE IF NOT EXISTS accounts (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    parent TEXT,
    type TEXT NOT NULL DEFAULT 'leaf'
  );
  CREATE TABLE IF NOT EXISTS org_units (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    parent TEXT,
    type TEXT NOT NULL DEFAULT 'leaf'
  );
  CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    source TEXT NOT NULL,       -- 'prod_a' or 'erp'
    canonical_id TEXT,
    erp_id TEXT
  );
  CREATE TABLE IF NOT EXISTS budget_versions (
    id TEXT PRIMARY KEY,
    prod_a_id TEXT REFERENCES projects(id),
    name TEXT NOT NULL,
    year TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    org_root TEXT,
    created_at TEXT NOT NULL,
    opened_at TEXT,
    submitted_at TEXT
  );
  CREATE TABLE IF NOT EXISTS budget_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id TEXT NOT NULL REFERENCES budget_versions(id),
    prod_a_id TEXT NOT NULL REFERENCES projects(id),
    account TEXT NOT NULL,
    org_unit TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL DEFAULT 'SEK',
    period TEXT NOT NULL,
    activity TEXT,
    cost_bearer TEXT,
    counterpart TEXT,
    dim1 TEXT,
    dim2 TEXT,
    dim3 TEXT
  );

  CREATE TABLE IF NOT EXISTS budget_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id TEXT NOT NULL REFERENCES budget_versions(id),
    org_unit TEXT NOT NULL,
    user_id TEXT NOT NULL,
    user_name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    completed_at TEXT
  );

  CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS dim_members (
    dimension TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (dimension, code)
  );
`);

// ── Kafka ──

const kafka = new Kafka({
  clientId: "product-a",
  brokers: [process.env.KAFKA_BROKER || "localhost:19092"],
});
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
const consumer = kafka.consumer({ groupId: "product-a-consumer" });

// ── Egress topics consumed by Product A ──

const CONSUME_TOPICS = [
  "platform.accounts.out",
  "platform.projects.out",
  "platform.links.out",
];

async function startConsumer() {
  await consumer.connect();
  for (const topic of CONSUME_TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: true });
  }
  console.log("[PROD-A] Subscribed to:", CONSUME_TOPICS.join(", "));

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      const data = JSON.parse(message.value.toString());

      // Idempotency: skip already-processed events
      const eventId = data.event_id || data.original?.event_id;
      if (eventId) {
        const already = db.prepare("SELECT 1 FROM processed_events WHERE event_id = ?").get(eventId);
        if (already) {
          console.log(`[PROD-A] Skipping duplicate event: ${eventId}`);
          return;
        }
      }

      console.log(`[PROD-A] ← ${topic}: ${data.event_type || data.original?.event_type || "enriched"}`);

      switch (topic) {
        case "platform.accounts.out": {
          // Referensdata
          const stmt = db.prepare("INSERT OR REPLACE INTO accounts (code, name, parent, type) VALUES (?, ?, ?, ?)");
          const stmtOrg = db.prepare("INSERT OR REPLACE INTO org_units (code, name, parent, type) VALUES (?, ?, ?, ?)");
          for (const acc of data.accounts || []) stmt.run(acc.code, acc.name, acc.parent || null, acc.type || 'leaf');
          for (const org of data.org_units || []) stmtOrg.run(org.code, org.name, org.parent || null, org.type || 'leaf');
          console.log("[PROD-A] Accounts & org_units updated");
          break;
        }
        case "platform.projects.out": {
          const orig = data.original;
          if (orig?.event_type === "ProjectCreated") {
            // ERP project
            db.prepare(
              "INSERT OR REPLACE INTO projects (id, name, source, canonical_id, erp_id) VALUES (?, ?, 'erp', ?, ?)"
            ).run(orig.erp_id, orig.name, data.canonical_id, orig.erp_id);
            console.log(`[PROD-A] ERP project stored: ${orig.erp_id}`);
          }
          break;
        }
        case "platform.links.out": {
          // Update canonical_id and erp_id on our local project
          if (data.linked?.prod_a && data.linked?.erp) {
            db.prepare(
              "UPDATE projects SET canonical_id = ?, erp_id = ? WHERE id = ?"
            ).run(data.canonical_id, data.linked.erp, data.linked.prod_a);
            console.log(`[PROD-A] Project ${data.linked.prod_a} linked to ERP ${data.linked.erp}`);
          }
          break;
        }
      }

      // Mark event as processed (idempotency)
      if (eventId) {
        db.prepare("INSERT OR IGNORE INTO processed_events (event_id, processed_at) VALUES (?, ?)").run(eventId, new Date().toISOString());
      }
    },
  });
}

// ── Dim member auto-registration ──\nconst upsertDimMember = db.prepare(\"INSERT OR IGNORE INTO dim_members (dimension, code, name) VALUES (?, ?, '')\");\nfunction registerDimMembers(lines: Record<string, unknown>[]) {\n  for (const line of lines) {\n    for (const dim of [\"dim1\", \"dim2\", \"dim3\"]) {\n      const val = line[dim];\n      if (val && typeof val === \"string\" && val.trim()) {\n        upsertDimMember.run(dim, val.trim());\n      }\n    }\n  }\n}\n\n// ── Express ──

const app = express();
app.use(cors({ origin: true, credentials: true }));
app.use(express.json());

// ── API Versioning ──
app.use((req, _res, next) => {
  if (req.path.startsWith("/api/v1/")) {
    req.url = req.url.replace("/api/v1/", "/api/");
  }
  next();
});
app.use(express.static(path.join(__dirname, "..", "public")));

// GET /api/accounts — reference data
app.get("/api/accounts", (_req, res) => {
  const accounts = db.prepare("SELECT * FROM accounts").all();
  const org_units = db.prepare("SELECT * FROM org_units").all();
  res.json({ accounts, org_units });
});

// GET /api/projects — all projects (own + ERP)
app.get("/api/projects", (_req, res) => {
  const projects = db.prepare("SELECT * FROM projects").all();
  res.json(projects);
});

// POST /api/projects — create budget project
app.post("/api/projects", async (req, res) => {
  const { name } = req.body;
  const count = (db.prepare("SELECT COUNT(*) as c FROM projects WHERE source = 'prod_a'").get() as any).c;
  const prodAId = `A${String(count + 1).padStart(4, '0')}`;
  const event = {
    event_id: uuid(),
    event_type: "BudgetProjectCreated",
    timestamp: new Date().toISOString(),
    source_system: "prod_a",
    prod_a_id: prodAId,
    name: name || "New budget project",
  };

  // Save locally
  db.prepare(
    "INSERT INTO projects (id, name, source, canonical_id) VALUES (?, ?, 'prod_a', NULL)"
  ).run(prodAId, event.name);

  // Publish to Kafka
  await producer.send({
    topic: "product-a.events",
    messages: [{ key: event.event_id, value: JSON.stringify(event) }],
  });
  console.log(`[PROD-A] Created budget project ${prodAId} → product-a.events`);
  res.json({ ok: true, event });
});

// GET /api/budget/:prodAId — get budget lines (optionally ?year=XXXX)
app.get("/api/budget/:prodAId", (req, res) => {
  const year = req.query.year as string | undefined;
  let lines;
  if (year) {
    lines = db.prepare(
      `SELECT bl.* FROM budget_lines bl
       JOIN budget_versions bv ON bl.version_id = bv.id
       WHERE bl.prod_a_id = ? AND bv.year = ?`
    ).all(req.params.prodAId, year);
  } else {
    lines = db.prepare("SELECT * FROM budget_lines WHERE prod_a_id = ?").all(req.params.prodAId);
  }
  res.json(lines);
});

// GET /api/budget-versions — list all budget versions
app.get("/api/budget-versions", (_req, res) => {
  const versions = db.prepare("SELECT * FROM budget_versions ORDER BY created_at DESC").all();
  res.json(versions);
});

// GET /api/budget-versions/:prodAId — get versions for project
app.get("/api/budget-versions/:prodAId", (req, res) => {
  const versions = db.prepare("SELECT * FROM budget_versions WHERE prod_a_id = ? ORDER BY created_at DESC").all(req.params.prodAId);
  res.json(versions);
});

// POST /api/budget — save budget lines (draft — no Kafka publishing)
app.post("/api/budget", async (req, res) => {
  const { prod_a_id, lines, year } = req.body;
  if (!prod_a_id || !lines?.length) {
    res.status(400).json({ error: "prod_a_id and lines required" });
    return;
  }

  const budgetYear = year || lines[0]?.period?.slice(0, 4) || "2025";

  // Find or create version for this project+year
  let version = db.prepare(
    "SELECT * FROM budget_versions WHERE prod_a_id = ? AND year = ?"
  ).get(prod_a_id, budgetYear) as any;

  if (!version) {
    const versionId = `ver-${prod_a_id}-${budgetYear}`;
    db.prepare(
    "INSERT INTO budget_versions (id, prod_a_id, name, year, status, created_at) VALUES (?, ?, ?, ?, 'draft', ?)"
  ).run(versionId, prod_a_id, `Budget ${budgetYear}`, budgetYear, new Date().toISOString());
    version = { id: versionId, status: "draft" };
  }

  if (version.status === "published") {
    res.status(400).json({ error: "Version is published and locked — reopen first" });
    return;
  }

  // Clear existing lines for this version and re-insert
  db.prepare("DELETE FROM budget_lines WHERE version_id = ?").run(version.id);

  const stmt = db.prepare(
    "INSERT INTO budget_lines (version_id, prod_a_id, account, org_unit, amount, currency, period, dim1, dim2, dim3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  );
  for (const line of lines) {
    stmt.run(version.id, prod_a_id, line.account, line.org_unit, line.amount, line.currency || "SEK", line.period, line.dim1 || null, line.dim2 || null, line.dim3 || null);
  }
  registerDimMembers(lines);

  console.log(`[PROD-A] Budget saved as draft for ${prod_a_id} (${version.id}) — ${lines.length} lines, NO Kafka event`);
  res.json({ ok: true, version_id: version.id, status: "draft", lines_count: lines.length });
});

// POST /api/budget-versions/:id/submit — publish budget data to Kafka (version stays editable)
app.post("/api/budget-versions/:id/submit", async (req, res) => {
  const version = db.prepare("SELECT * FROM budget_versions WHERE id = ?").get(req.params.id) as any;
  if (!version) {
    res.status(404).json({ error: "Version not found" });
    return;
  }
  if (version.status === "published") {
    res.status(400).json({ error: "Version is published and locked — reopen first" });
    return;
  }

  const lines = db.prepare("SELECT * FROM budget_lines WHERE version_id = ?").all(version.id) as any[];
  if (lines.length === 0) {
    res.status(400).json({ error: "No budget lines to submit" });
    return;
  }

  // Update status to submitted
  if (version.status === "draft" || version.status === "open") {
    db.prepare(
      "UPDATE budget_versions SET status = 'submitted', submitted_at = ? WHERE id = ?"
    ).run(new Date().toISOString(), version.id);
  }

  // Publish to Kafka
  const event = {
    event_id: uuid(),
    event_type: "BudgetSubmitted",
    timestamp: new Date().toISOString(),
    source_system: "prod_a",
    prod_a_id: version.prod_a_id,
    version_id: version.id,
    version_name: version.name,
    year: version.year,
    lines: lines.map((l: any) => ({
      account: l.account,
      org_unit: l.org_unit,
      amount: l.amount,
      currency: l.currency,
      period: l.period,
      dim1: l.dim1 || undefined,
      dim2: l.dim2 || undefined,
      dim3: l.dim3 || undefined,
    })),
  };
  await producer.send({
    topic: "product-a.events",
    messages: [{ key: event.event_id, value: JSON.stringify(event) }],
  });

  console.log(`[PROD-A] Budget SUBMITTED: ${version.id} (${lines.length} lines) → product-a.events`);
  res.json({ ok: true, status: "submitted", event });
});

// POST /api/budget-versions/:id/publish — publish and lock version
app.post("/api/budget-versions/:id/publish", async (req, res) => {
  const version = db.prepare("SELECT * FROM budget_versions WHERE id = ?").get(req.params.id) as any;
  if (!version) {
    res.status(404).json({ error: "Version not found" });
    return;
  }
  if (version.status === "published") {
    res.status(400).json({ error: "Already published" });
    return;
  }

  const lines = db.prepare("SELECT * FROM budget_lines WHERE version_id = ?").all(version.id) as any[];
  if (lines.length === 0) {
    res.status(400).json({ error: "No budget lines to publish" });
    return;
  }

  // Lock version
  db.prepare(
    "UPDATE budget_versions SET status = 'published', submitted_at = ? WHERE id = ?"
  ).run(new Date().toISOString(), version.id);

  // Publish to Kafka
  const event = {
    event_id: uuid(),
    event_type: "BudgetSubmitted",
    timestamp: new Date().toISOString(),
    source_system: "prod_a",
    prod_a_id: version.prod_a_id,
    version_id: version.id,
    version_name: version.name,
    year: version.year,
    lines: lines.map((l: any) => ({
      account: l.account,
      org_unit: l.org_unit,
      amount: l.amount,
      currency: l.currency,
      period: l.period,
      dim1: l.dim1 || undefined,
      dim2: l.dim2 || undefined,
      dim3: l.dim3 || undefined,
    })),
  };
  await producer.send({
    topic: "product-a.events",
    messages: [{ key: event.event_id, value: JSON.stringify(event) }],
  });

  console.log(`[PROD-A] Budget PUBLISHED & LOCKED: ${version.id} (${lines.length} lines) → product-a.events`);

  // Publish task event
  const taskEvent = {
    event_id: uuid(),
    event_type: "TaskAssigned",
    timestamp: new Date().toISOString(),
    source: "prod_a",
    task_id: `budget-review-${version.id}`,
    task_type: "approval",
    category: "approval",
    title: `Review budget: ${version.name}`,
    description: `${lines.length} budget entries published for ${version.name}. Version is now locked.`,
    priority: "high",
    assigned_to: "user-001",
    task_path: `/#budget/${version.prod_a_id}`,
  };
  await producer.send({
    topic: "product-a.tasks",
    messages: [{ key: taskEvent.event_id, value: JSON.stringify(taskEvent) }],
  });
  console.log(`[PROD-A] Task published: "${taskEvent.title}" → product-a.tasks`);

  res.json({ ok: true, status: "published", event });
});

// POST /api/budget-versions/:id/reopen — reopen published/submitted budget back to open
app.post("/api/budget-versions/:id/reopen", (_req, res) => {
  const version = db.prepare("SELECT * FROM budget_versions WHERE id = ?").get(_req.params.id) as any;
  if (!version) { res.status(404).json({ error: "Version not found" }); return; }
  if (version.status !== "published" && version.status !== "submitted") {
    res.status(400).json({ error: "Only published/submitted versions can be reopened" });
    return;
  }
  db.prepare("UPDATE budget_versions SET status = 'open', submitted_at = NULL WHERE id = ?").run(version.id);
  console.log(`[PROD-A] Budget REOPENED: ${version.id}`);
  res.json({ ok: true, version_id: version.id, status: "open" });
});

// POST /api/budget-versions — create a new budget version (from Process Management)
app.post("/api/budget-versions", (req, res) => {
  const { name, year, org_root } = req.body;
  if (!name || !year) { res.status(400).json({ error: "name and year required" }); return; }
  const id = `bv-${Date.now()}`;
  db.prepare(
    "INSERT INTO budget_versions (id, prod_a_id, name, year, status, org_root, created_at) VALUES (?, ?, ?, ?, 'draft', ?, ?)"
  ).run(id, "", name, year, org_root || null, new Date().toISOString());
  console.log(`[PROD-A] Budget version created: ${id} "${name}" year=${year} org_root=${org_root}`);
  res.json({ ok: true, id, name, year, status: "draft", org_root });
});

// ═══ Assignments API (on budget versions) ═══

// GET /api/budget-versions/:id/assignments
app.get("/api/budget-versions/:id/assignments", (req, res) => {
  const assignments = db.prepare(
    "SELECT * FROM budget_assignments WHERE version_id = ? ORDER BY org_unit, user_name"
  ).all(req.params.id);
  res.json(assignments);
});

// PUT /api/budget-versions/:id/org-root — set org_root on existing version
app.put("/api/budget-versions/:id/org-root", (req, res) => {
  const version = db.prepare("SELECT * FROM budget_versions WHERE id = ?").get(req.params.id) as any;
  if (!version) { res.status(404).json({ error: "Version not found" }); return; }
  const { org_root } = req.body;
  db.prepare("UPDATE budget_versions SET org_root = ? WHERE id = ?").run(org_root || null, req.params.id);
  res.json({ ok: true });
});

// POST /api/budget-versions/:id/assignments — bulk set assignments
app.post("/api/budget-versions/:id/assignments", async (req, res) => {
  const version = db.prepare("SELECT * FROM budget_versions WHERE id = ?").get(req.params.id) as any;
  if (!version) { res.status(404).json({ error: "Version not found" }); return; }
  if (version.status === "published") { res.status(400).json({ error: "Cannot edit assignments on published version" }); return; }

  const { assignments } = req.body;
  if (!Array.isArray(assignments)) { res.status(400).json({ error: "assignments array required" }); return; }

  // Preserve completion status for existing assignments when re-saving
  const existing = db.prepare("SELECT org_unit, user_id, status, completed_at FROM budget_assignments WHERE version_id = ?").all(req.params.id) as any[];
  const statusMap: Record<string, { status: string; completed_at: string | null }> = {};
  for (const e of existing) {
    statusMap[`${e.org_unit}:${e.user_id}`] = { status: e.status, completed_at: e.completed_at };
  }

  // Detect new assignments for task publishing
  const existingKeys = new Set(existing.map(e => `${e.org_unit}:${e.user_id}`));
  const newAssignments = assignments.filter((a: any) => a.org_unit && a.user_id && !existingKeys.has(`${a.org_unit}:${a.user_id}`));

  // Replace all assignments
  db.prepare("DELETE FROM budget_assignments WHERE version_id = ?").run(req.params.id);
  const stmt = db.prepare(
    "INSERT INTO budget_assignments (version_id, org_unit, user_id, user_name, status, completed_at) VALUES (?, ?, ?, ?, ?, ?)"
  );
  for (const a of assignments) {
    if (a.org_unit && a.user_id) {
      const prev = statusMap[`${a.org_unit}:${a.user_id}`];
      stmt.run(req.params.id, a.org_unit, a.user_id, a.user_name || "", prev?.status || "pending", prev?.completed_at || null);
    }
  }

  // If version is already open, publish tasks for NEW assignments immediately
  let tasksCreated = 0;
  if (version.status === "open" && newAssignments.length > 0) {
    for (const a of newAssignments) {
      const orgRow = db.prepare("SELECT name, type FROM org_units WHERE code = ?").get(a.org_unit) as any;
      const orgName = orgRow ? orgRow.name : a.org_unit;
      const isGroup = orgRow && orgRow.type === "group";
      const taskEvent = {
        event_id: uuid(),
        event_type: "TaskAssigned",
        timestamp: new Date().toISOString(),
        source: "prod_a",
        task_id: `budget-${req.params.id}-${a.org_unit}-${a.user_id}`,
        task_type: "budget_entry",
        category: "action",
        title: isGroup ? `${version.name}: ${orgName} (group)` : `${version.name}: ${orgName}`,
        description: isGroup
          ? `Budget entry for ${orgName} and all sub-units — ${version.name} (${version.year})`
          : `Budget entry for ${orgName} — ${version.name} (${version.year})`,
        priority: "normal",
        assigned_to: a.user_id,
        task_path: `/?version=${req.params.id}&org=${a.org_unit}`,
      };
      await producer.send({
        topic: "product-a.tasks",
        messages: [{ key: taskEvent.event_id, value: JSON.stringify(taskEvent) }],
      });
      tasksCreated++;
      console.log(`[PROD-A] New task published for ${a.user_name || a.user_id} (${a.org_unit}) on open version`);
    }
  }

  console.log(`[PROD-A] Assignments updated: ${assignments.length} for ${req.params.id}${tasksCreated ? `, ${tasksCreated} new tasks` : ""}`);
  res.json({ ok: true, count: assignments.length, new_tasks: tasksCreated });
});

// PUT /api/budget-versions/:id/open — transition Draft/Submitted → Open, publish tasks
app.put("/api/budget-versions/:id/open", async (req, res) => {
  const version = db.prepare("SELECT * FROM budget_versions WHERE id = ?").get(req.params.id) as any;
  if (!version) { res.status(404).json({ error: "Version not found" }); return; }
  if (version.status === "published") { res.status(400).json({ error: "Published versions must be reopened first" }); return; }
  if (version.status === "open") { res.status(400).json({ error: "Version is already open" }); return; }

  const assignments = db.prepare("SELECT * FROM budget_assignments WHERE version_id = ?").all(req.params.id) as any[];
  if (assignments.length === 0) { res.status(400).json({ error: "Add at least one assignment before opening" }); return; }

  db.prepare("UPDATE budget_versions SET status = 'open', opened_at = datetime('now') WHERE id = ?").run(req.params.id);

  // Publish one task per assignment
  for (const a of assignments) {
    const orgRow = db.prepare("SELECT name, type FROM org_units WHERE code = ?").get(a.org_unit) as any;
    const orgName = orgRow ? orgRow.name : a.org_unit;
    const isGroup = orgRow && orgRow.type === "group";
    const taskEvent = {
      event_id: uuid(),
      event_type: "TaskAssigned",
      timestamp: new Date().toISOString(),
      source: "prod_a",
      task_id: `budget-${req.params.id}-${a.org_unit}-${a.user_id}`,
      task_type: "budget_entry",
      category: "action",
      title: isGroup ? `${version.name}: ${orgName} (group)` : `${version.name}: ${orgName}`,
      description: isGroup
        ? `Budget entry for ${orgName} and all sub-units — ${version.name} (${version.year})`
        : `Budget entry for ${orgName} — ${version.name} (${version.year})`,
      priority: "normal",
      assigned_to: a.user_id,
      task_path: `/?version=${req.params.id}&org=${a.org_unit}`,
    };
    await producer.send({
      topic: "product-a.tasks",
      messages: [{ key: taskEvent.event_id, value: JSON.stringify(taskEvent) }],
    });
    console.log(`[PROD-A] Task published for ${a.user_name} (${a.org_unit})`);
  }

  console.log(`[PROD-A] Budget version OPENED: ${req.params.id} (${assignments.length} tasks)`);
  res.json({ ok: true, status: "open", tasks_created: assignments.length });
});

// DELETE /api/budget-versions/:id — delete draft version
app.delete("/api/budget-versions/:id", (_req, res) => {
  const version = db.prepare("SELECT * FROM budget_versions WHERE id = ?").get(_req.params.id) as any;
  if (!version) { res.status(404).json({ error: "Version not found" }); return; }
  if (version.status !== "draft") { res.status(400).json({ error: "Only draft versions can be deleted" }); return; }

  db.prepare("DELETE FROM budget_assignments WHERE version_id = ?").run(_req.params.id);
  db.prepare("DELETE FROM budget_lines WHERE version_id = ?").run(_req.params.id);
  db.prepare("DELETE FROM budget_versions WHERE id = ?").run(_req.params.id);
  console.log(`[PROD-A] Budget version DELETED: ${_req.params.id}`);
  res.json({ ok: true });
});

// PUT /api/budget-assignments/:id/complete — mark assignment as done
app.put("/api/budget-assignments/:id/complete", async (req, res) => {
  const assignment = db.prepare("SELECT * FROM budget_assignments WHERE id = ?").get(req.params.id) as any;
  if (!assignment) { res.status(404).json({ error: "Assignment not found" }); return; }

  db.prepare("UPDATE budget_assignments SET status = 'done', completed_at = datetime('now') WHERE id = ?").run(req.params.id);

  // Publish TaskCompleted so platform inbox updates
  const taskId = `budget-${assignment.version_id}-${assignment.org_unit}-${assignment.user_id}`;
  await producer.send({
    topic: "product-a.tasks",
    messages: [{ key: uuid(), value: JSON.stringify({
      event_id: uuid(), event_type: "TaskCompleted", timestamp: new Date().toISOString(),
      source: "prod_a", task_id: taskId,
    })}],
  });

  console.log(`[PROD-A] Assignment completed: ${req.params.id} → TaskCompleted`);
  res.json({ ok: true, status: "done" });
});

// PUT /api/budget-assignments/:id/reopen — reopen a completed assignment
app.put("/api/budget-assignments/:id/reopen", (req, res) => {
  const assignment = db.prepare("SELECT * FROM budget_assignments WHERE id = ?").get(req.params.id) as any;
  if (!assignment) { res.status(404).json({ error: "Assignment not found" }); return; }

  db.prepare("UPDATE budget_assignments SET status = 'pending', completed_at = NULL WHERE id = ?").run(req.params.id);
  console.log(`[PROD-A] Assignment reopened: ${req.params.id}`);
  res.json({ ok: true, status: "pending" });
});

// PUT /api/budget-versions/:versionId/assignments/:org/complete — complete by version+org (used from budget entry)
app.put("/api/budget-versions/:versionId/assignments/:org/complete", async (req, res) => {
  const assignment = db.prepare(
    "SELECT * FROM budget_assignments WHERE version_id = ? AND org_unit = ?"
  ).get(req.params.versionId, req.params.org) as any;
  if (!assignment) { res.status(404).json({ error: "Assignment not found" }); return; }

  db.prepare("UPDATE budget_assignments SET status = 'done', completed_at = datetime('now') WHERE id = ?").run(assignment.id);

  // Publish TaskCompleted so platform inbox updates
  const taskId = `budget-${assignment.version_id}-${assignment.org_unit}-${assignment.user_id}`;
  await producer.send({
    topic: "product-a.tasks",
    messages: [{ key: uuid(), value: JSON.stringify({
      event_id: uuid(), event_type: "TaskCompleted", timestamp: new Date().toISOString(),
      source: "prod_a", task_id: taskId,
    })}],
  });

  console.log(`[PROD-A] Assignment completed via budget entry: ${assignment.id} → TaskCompleted`);
  res.json({ ok: true, status: "done", assignment_id: assignment.id });
});

// Healthcheck
app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "product-a" });
});

// Capabilities — Platform discovers this system's dimensions automatically
app.get("/api/capabilities", (_req, res) => {
  res.json({
    system_name: "prod_a",
    system_type: "budgeting",
    display_name: "Product A",
    task_base_url: "http://localhost:3002",
    data_fields: [
      { field_name: "account", field_label: "Account", shared_dimension: "account", role: "both" },
      { field_name: "org_unit", field_label: "Org Unit", shared_dimension: "org_unit", role: "both" },
      { field_name: "project", field_label: "Project", shared_dimension: "project", role: "producer" },
      { field_name: "dim1", field_label: "Flex-dim 1", data_type: "string" },
      { field_name: "dim2", field_label: "Flex-dim 2", data_type: "string" },
      { field_name: "dim3", field_label: "Flex-dim 3", data_type: "string" }
    ]
  });
});

app.post("/api/reset", (_req, res) => {
  db.exec("DELETE FROM processed_events; DELETE FROM budget_assignments; DELETE FROM budget_lines; DELETE FROM budget_versions; DELETE FROM projects; DELETE FROM accounts; DELETE FROM org_units; DELETE FROM dim_members;");
  console.log("[PROD-A] All data reset");
  res.json({ ok: true });
});

// GET /api/dim-members — flex-dimension members
app.get("/api/dim-members", (_req, res) => {
  const rows = db.prepare("SELECT dimension, code, name FROM dim_members ORDER BY dimension, code").all();
  res.json(rows);
});

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "index.html"));
});

// ── Start ──

const PORT = 3002;

async function start() {
  await producer.connect();
  console.log("[PROD-A] Kafka producer connected");
  await startConsumer();

  app.listen(PORT, () => {
    console.log(`[PROD-A] Product A running on http://localhost:${PORT}`);
  });
}

start().catch((err) => {
  console.error("[PROD-A] Failed to start:", err);
  process.exit(1);
});
