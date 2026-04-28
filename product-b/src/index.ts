import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";
import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

// ── Database ──

const DB_PATH = path.join(__dirname, "..", "data", "product-b.db");
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
    canonical_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    source TEXT NOT NULL,
    erp_id TEXT,
    prod_a_id TEXT
  );
  CREATE TABLE IF NOT EXISTS budget_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_id TEXT NOT NULL,
    account TEXT NOT NULL,
    org_unit TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL DEFAULT 'SEK',
    period TEXT NOT NULL,
    dim1 TEXT,
    dim2 TEXT,
    dim3 TEXT,
    dim4 TEXT,
    dim5 TEXT,
    planning_year TEXT,
    planning_type TEXT,
    planning_version INTEGER
  );
  CREATE TABLE IF NOT EXISTS gl_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_id TEXT NOT NULL,
    account TEXT NOT NULL,
    org_unit TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL DEFAULT 'SEK',
    period TEXT NOT NULL,
    transaction_date TEXT,
    dim1 TEXT,
    dim2 TEXT,
    dim3 TEXT,
    dim4 TEXT,
    dim5 TEXT
  );

  CREATE TABLE IF NOT EXISTS ingestion_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_name TEXT NOT NULL,
    source_type TEXT NOT NULL DEFAULT 'all',  -- 'budget', 'gl', or 'all'
    rule_type TEXT NOT NULL,                  -- 'default', 'derive', 'map'
    condition_field TEXT,                     -- field to check (e.g. 'account', 'dim3')
    condition_op TEXT,                        -- 'IS NULL', 'LIKE', '=', 'IN'
    condition_value TEXT,                     -- value for condition (e.g. '3%', 'OU-300')
    target_field TEXT NOT NULL,               -- field to set (e.g. 'dim2', 'dim3')
    target_value TEXT NOT NULL,               -- value to set
    priority INTEGER NOT NULL DEFAULT 100,    -- lower = runs first
    enabled INTEGER NOT NULL DEFAULT 1
  );
`);

// ── Ingestion rules engine ──

// Seed default rules if table is empty
const ruleCount = (db.prepare("SELECT COUNT(*) AS cnt FROM ingestion_rules").get() as { cnt: number }).cnt;
if (ruleCount === 0) {
  const seed = db.prepare(
    "INSERT INTO ingestion_rules (rule_name, source_type, rule_type, condition_field, condition_op, condition_value, target_field, target_value, priority) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
  );
  // Default: if dim3 is null, set to 'STANDARD'
  seed.run("Default dim3", "all", "default", "dim3", "IS NULL", null, "dim3", "STANDARD", 100);
  // Derive: account starting with 3 → dim2 = 'REVENUE'
  seed.run("Revenue accounts → dim2", "all", "derive", "account", "LIKE", "3%", "dim2", "REVENUE", 50);
  // Derive: account starting with 4 → dim2 = 'PERSONNEL'
  seed.run("Personnel accounts → dim2", "all", "derive", "account", "LIKE", "4%", "dim2", "PERSONNEL", 51);
  // Derive: account starting with 5 → dim2 = 'EXTERNAL'
  seed.run("External cost accounts → dim2", "all", "derive", "account", "LIKE", "5%", "dim2", "EXTERNAL", 52);
  // Derive: account starting with 6 → dim2 = 'OPERATIONS'
  seed.run("Operating cost accounts → dim2", "all", "derive", "account", "LIKE", "6%", "dim2", "OPERATIONS", 53);
  console.log("[PROD-B] Seeded 5 default ingestion rules");
}

interface IngestionRule {
  id: number;
  rule_name: string;
  source_type: string;
  rule_type: string;
  condition_field: string | null;
  condition_op: string | null;
  condition_value: string | null;
  target_field: string;
  target_value: string;
  priority: number;
  enabled: number;
}

function applyIngestionRules(line: Record<string, unknown>, sourceType: "budget" | "gl"): Record<string, unknown> {
  const rules = db.prepare(
    "SELECT * FROM ingestion_rules WHERE enabled = 1 AND (source_type = ? OR source_type = 'all') ORDER BY priority ASC"
  ).all(sourceType) as IngestionRule[];

  const result = { ...line };

  for (const rule of rules) {
    const fieldValue = result[rule.condition_field || ""] as string | null | undefined;

    let matches = false;
    switch (rule.condition_op) {
      case "IS NULL":
        matches = fieldValue == null || fieldValue === "";
        break;
      case "=":
        matches = fieldValue === rule.condition_value;
        break;
      case "LIKE": {
        // Simple LIKE: supports trailing % only (prefix match)
        const pattern = rule.condition_value || "";
        if (pattern.endsWith("%")) {
          matches = typeof fieldValue === "string" && fieldValue.startsWith(pattern.slice(0, -1));
        } else {
          matches = fieldValue === pattern;
        }
        break;
      }
      case "IN": {
        const values = (rule.condition_value || "").split(",").map((v) => v.trim());
        matches = values.includes(String(fieldValue));
        break;
      }
      default:
        matches = false;
    }

    if (matches) {
      // 'default' only sets if target is currently null/empty
      if (rule.rule_type === "default") {
        const current = result[rule.target_field];
        if (current == null || current === "") {
          result[rule.target_field] = rule.target_value;
        }
      } else {
        // 'derive' and 'map' always overwrite
        result[rule.target_field] = rule.target_value;
      }
    }
  }

  return result;
}

// ── Kafka consumer ──

const kafka = new Kafka({
  clientId: "product-b",
  brokers: [process.env.KAFKA_BROKER || "localhost:19092"],
});
const consumer = kafka.consumer({ groupId: "product-b-consumer" });

const CONSUME_TOPICS = [
  "platform.accounts.out",
  "platform.projects.out",
  "platform.budget.out",
  "platform.gl.out",
  "platform.links.out",
];

async function startConsumer() {
  await consumer.connect();
  for (const topic of CONSUME_TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: true });
  }
  console.log("[PROD-B] Subscribed to:", CONSUME_TOPICS.join(", "));

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      const data = JSON.parse(message.value.toString());
      console.log(`[PROD-B] ← ${topic}: ${data.event_type || data.original?.event_type || "enriched"}`);

      switch (topic) {
        case "platform.accounts.out": {
          const stmt = db.prepare("INSERT OR REPLACE INTO accounts (code, name, parent, type) VALUES (?, ?, ?, ?)");
          const stmtOrg = db.prepare("INSERT OR REPLACE INTO org_units (code, name, parent, type) VALUES (?, ?, ?, ?)");
          for (const acc of data.accounts || []) stmt.run(acc.code, acc.name, acc.parent || null, acc.type || 'leaf');
          for (const org of data.org_units || []) stmtOrg.run(org.code, org.name, org.parent || null, org.type || 'leaf');
          console.log("[PROD-B] Accounts & org_units updated");
          break;
        }

        case "platform.projects.out": {
          const orig = data.original;
          if (orig?.event_type === "ProjectCreated") {
            db.prepare(
              "INSERT OR REPLACE INTO projects (canonical_id, name, source, erp_id) VALUES (?, ?, 'erp', ?)"
            ).run(data.canonical_id, orig.name, orig.erp_id);
            console.log(`[PROD-B] ERP project stored: ${data.canonical_id}`);
          } else if (orig?.event_type === "BudgetProjectCreated") {
            db.prepare(
              "INSERT OR REPLACE INTO projects (canonical_id, name, source, prod_a_id) VALUES (?, ?, 'prod_a', ?)"
            ).run(data.canonical_id, orig.name, orig.prod_a_id);
            console.log(`[PROD-B] Budget project stored: ${data.canonical_id}`);
          }
          break;
        }

        case "platform.budget.out": {
          // Budget from Product A — Platform has enriched with planning_dimensions + dim_values_per_line
          const orig = data.original;
          const dims = data.planning_dimensions;
          const dimPerLine = data.dim_values_per_line || [];

          // Clear previous budget lines for this canonical_id + year to handle re-submissions
          const pYear = dims?.planning_year || orig?.year || null;
          if (pYear) {
            db.prepare("DELETE FROM budget_lines WHERE canonical_id = ? AND planning_year = ?")
              .run(data.canonical_id, pYear);
          } else {
            db.prepare("DELETE FROM budget_lines WHERE canonical_id = ?")
              .run(data.canonical_id);
          }

          const stmt = db.prepare(
            "INSERT INTO budget_lines (canonical_id, account, org_unit, amount, currency, period, dim1, dim2, dim3, dim4, dim5, planning_year, planning_type, planning_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
          );
          const budgetLines = orig?.lines || [];
          let rulesApplied = 0;
          for (let i = 0; i < budgetLines.length; i++) {
            const line = budgetLines[i];
            const dv = dimPerLine[i] || {};
            // Build a merged line object for rule evaluation
            const merged = {
              account: line.account, org_unit: line.org_unit, amount: line.amount,
              currency: line.currency || "SEK", period: line.period,
              dim1: dv.dim1 || null, dim2: dv.dim2 || null, dim3: dv.dim3 || null,
              dim4: dv.dim4 || null, dim5: dv.dim5 || null,
            };
            // Apply product-specific ingestion rules
            const enriched = applyIngestionRules(merged, "budget") as Record<string, unknown>;
            if (JSON.stringify(merged) !== JSON.stringify(enriched)) rulesApplied++;
            stmt.run(
              data.canonical_id, enriched.account, enriched.org_unit, enriched.amount, enriched.currency, enriched.period,
              enriched.dim1 || null, enriched.dim2 || null, enriched.dim3 || null, enriched.dim4 || null, enriched.dim5 || null,
              dims?.planning_year || null, dims?.planning_type || null, dims?.planning_version || null
            );
          }
          console.log(`[PROD-B] Budget lines stored for ${data.canonical_id} (${budgetLines.length} lines, year=${pYear}, ${rulesApplied} enriched by ingestion rules)`);
          break;
        }

        case "platform.gl.out": {
          // GL from ERP — Platform has enriched with dim_values_per_entry via dim_routing
          const orig = data.original;
          const dimPerEntry = data.dim_values_per_entry || [];
          const stmt = db.prepare(
            "INSERT INTO gl_lines (canonical_id, account, org_unit, amount, currency, period, transaction_date, dim1, dim2, dim3, dim4, dim5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
          );
          const entries = orig?.entries || [];
          let rulesApplied = 0;
          for (let i = 0; i < entries.length; i++) {
            const entry = entries[i];
            const dv = dimPerEntry[i] || {};
            // Build a merged line object for rule evaluation
            const merged = {
              account: entry.account, org_unit: entry.org_unit, amount: entry.amount,
              currency: entry.currency || "SEK", period: entry.period,
              transaction_date: entry.transaction_date || null,
              dim1: dv.dim1 || null, dim2: dv.dim2 || null, dim3: dv.dim3 || null,
              dim4: dv.dim4 || null, dim5: dv.dim5 || null,
            };
            // Apply product-specific ingestion rules
            const enriched = applyIngestionRules(merged, "gl") as Record<string, unknown>;
            if (JSON.stringify(merged) !== JSON.stringify(enriched)) rulesApplied++;
            stmt.run(data.canonical_id, enriched.account, enriched.org_unit, enriched.amount, enriched.currency, enriched.period,
              enriched.transaction_date || null, enriched.dim1 || null, enriched.dim2 || null, enriched.dim3 || null, enriched.dim4 || null, enriched.dim5 || null);
          }
          console.log(`[PROD-B] GL lines stored for ${data.canonical_id} (${entries.length} lines, ${rulesApplied} enriched by ingestion rules)`);
          break;
        }

        case "platform.links.out": {
          // Merge projects: consolidate all data under data.canonical_id
          if (data.linked?.erp && data.linked?.prod_a) {
            const newCid = data.canonical_id;

            // Find old canonical IDs for prod_a and erp projects
            const prodARow = db.prepare(
              "SELECT canonical_id FROM projects WHERE prod_a_id = ?"
            ).get(data.linked.prod_a) as { canonical_id: string } | undefined;
            const erpRow = db.prepare(
              "SELECT canonical_id FROM projects WHERE erp_id = ? AND source = 'erp'"
            ).get(data.linked.erp) as { canonical_id: string } | undefined;

            // Move budget_lines from old prod_a canonical to new
            if (prodARow && prodARow.canonical_id !== newCid) {
              db.prepare("UPDATE budget_lines SET canonical_id = ? WHERE canonical_id = ?")
                .run(newCid, prodARow.canonical_id);
              db.prepare("UPDATE projects SET canonical_id = ?, erp_id = ? WHERE canonical_id = ? AND source = 'prod_a'")
                .run(newCid, data.linked.erp, prodARow.canonical_id);
            } else {
              // prod_a project already has the right canonical_id, just add erp_id
              db.prepare("UPDATE projects SET erp_id = ? WHERE canonical_id = ? AND source = 'prod_a'")
                .run(data.linked.erp, newCid);
            }

            // Move gl_lines from old ERP canonical to new
            if (erpRow && erpRow.canonical_id !== newCid) {
              db.prepare("UPDATE gl_lines SET canonical_id = ? WHERE canonical_id = ?")
                .run(newCid, erpRow.canonical_id);
              db.prepare("DELETE FROM projects WHERE canonical_id = ? AND source = 'erp'")
                .run(erpRow.canonical_id);
            }

            console.log(`[PROD-B] Projects linked: ${newCid}`);
          }
          break;
        }
      }
    },
  });
}

// ── Express ──

const app = express();
app.use(cors({ origin: true, credentials: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, "..", "public")));

// GET /api/analytics — the main read model: budget vs actuals
// Returns separate rows for budget and GL so different dimensions show independently
app.get("/api/analytics", (_req, res) => {
  const rows = db.prepare(`
    SELECT
      p.canonical_id,
      p.name AS project_name,
      b.account,
      b.org_unit,
      b.period,
      b.currency,
      b.dim1,
      b.dim2,
      b.dim3,
      b.planning_year,
      b.planning_type,
      b.planning_version,
      'budget' AS source,
      NULL AS transaction_date,
      SUM(b.amount) AS amount
    FROM projects p
    JOIN budget_lines b ON p.canonical_id = b.canonical_id
    GROUP BY p.canonical_id, b.account, b.org_unit, b.period, b.dim1, b.dim2, b.dim3, b.planning_year, b.planning_type, b.planning_version
    UNION ALL
    SELECT
      p.canonical_id,
      p.name AS project_name,
      g.account,
      g.org_unit,
      g.period,
      g.currency,
      g.dim1,
      g.dim2,
      g.dim3,
      NULL AS planning_year,
      NULL AS planning_type,
      NULL AS planning_version,
      'actual' AS source,
      g.transaction_date,
      SUM(g.amount) AS amount
    FROM projects p
    JOIN gl_lines g ON p.canonical_id = g.canonical_id
    GROUP BY p.canonical_id, g.account, g.org_unit, g.period, g.dim1, g.dim2, g.dim3, g.transaction_date
    ORDER BY account, org_unit, project_name
  `).all();
  res.json(rows);
});

// GET /api/projects
app.get("/api/projects", (_req, res) => {
  const projects = db.prepare("SELECT * FROM projects").all();
  res.json(projects);
});

// GET /api/accounts
app.get("/api/accounts", (_req, res) => {
  const accounts = db.prepare("SELECT * FROM accounts").all();
  const org_units = db.prepare("SELECT * FROM org_units").all();
  res.json({ accounts, org_units });
});

// ── Ingestion Rules API ──

app.get("/api/ingestion-rules", (_req, res) => {
  const rules = db.prepare("SELECT * FROM ingestion_rules ORDER BY priority ASC").all();
  res.json(rules);
});

app.post("/api/ingestion-rules", (req, res) => {
  const { rule_name, source_type, rule_type, condition_field, condition_op, condition_value, target_field, target_value, priority } = req.body;
  if (!rule_name || !rule_type || !target_field || !target_value) {
    return res.status(400).json({ error: "rule_name, rule_type, target_field, target_value are required" });
  }
  const result = db.prepare(
    "INSERT INTO ingestion_rules (rule_name, source_type, rule_type, condition_field, condition_op, condition_value, target_field, target_value, priority) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
  ).run(rule_name, source_type || "all", rule_type, condition_field || null, condition_op || null, condition_value || null, target_field, target_value, priority ?? 100);
  res.json({ ok: true, id: result.lastInsertRowid });
});

app.put("/api/ingestion-rules/:id", (req, res) => {
  const { enabled } = req.body;
  if (enabled !== undefined) {
    db.prepare("UPDATE ingestion_rules SET enabled = ? WHERE id = ?").run(enabled ? 1 : 0, req.params.id);
  }
  res.json({ ok: true });
});

app.delete("/api/ingestion-rules/:id", (req, res) => {
  db.prepare("DELETE FROM ingestion_rules WHERE id = ?").run(req.params.id);
  res.json({ ok: true });
});

// Healthcheck
app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "product-b" });
});

// Capabilities — Platform discovers this system's dimensions automatically
app.get("/api/capabilities", (_req, res) => {
  res.json({
    system_name: "prod_b",
    system_type: "analytics",
    display_name: "Product B",
    task_base_url: "http://localhost:3003",
    data_fields: [
      { field_name: "account", field_label: "Account", shared_dimension: "account", role: "consumer" },
      { field_name: "org_unit", field_label: "Org Unit", shared_dimension: "org_unit", role: "consumer" },
      { field_name: "project", field_label: "Project", shared_dimension: "project", role: "consumer" },
      { field_name: "dim1", field_label: "Flex-dim 1", data_type: "string" },
      { field_name: "dim2", field_label: "Flex-dim 2", data_type: "string" },
      { field_name: "dim3", field_label: "Flex-dim 3", data_type: "string" },
      { field_name: "dim4", field_label: "Flex-dim 4", data_type: "string" },
      { field_name: "dim5", field_label: "Flex-dim 5", data_type: "string" }
    ]
  });
});

// GET /api/dim-labels — fetch dim model from Platform to display column labels
const PLATFORM_URL = process.env.PLATFORM_URL || "http://platform:3000";
app.get("/api/dim-labels", async (_req, res) => {
  try {
    const r = await fetch(`${PLATFORM_URL}/api/dim-model/prod_b`);
    const model = await r.json() as Record<string, string>;
    res.json(model);
  } catch {
    res.json({ dim1: "Dim 1", dim2: "Dim 2", dim3: "Dim 3", dim4: "Dim 4", dim5: "Dim 5" });
  }
});

app.post("/api/reset", (_req, res) => {
  db.exec("DELETE FROM budget_lines; DELETE FROM gl_lines; DELETE FROM projects; DELETE FROM accounts; DELETE FROM org_units; DELETE FROM ingestion_rules;");
  console.log("[PRODUCT-B] All data reset");
  res.json({ ok: true });
});

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "index.html"));
});

// ── Start ──

const PORT = 3003;

async function start() {
  await startConsumer();

  app.listen(PORT, () => {
    console.log(`[PROD-B] Product B running on http://localhost:${PORT}`);
  });
}

start().catch((err) => {
  console.error("[PROD-B] Failed to start:", err);
  process.exit(1);
});
