import express from "express";
import { Kafka, Partitioners } from "kafkajs";
import { v4 as uuid } from "uuid";

// ── Types (inline to avoid shared module complexity in Docker) ──

interface BaseEvent {
  event_id: string;
  event_type: string;
  timestamp: string;
  source_system: "erp";
}

const TOPICS = {
  ERP_PROJECTS: "erp.projects",
  ERP_ACCOUNTS: "erp.accounts",
  ERP_GENERAL_LEDGER: "erp.general-ledger",
};

// ── Kafka setup ──

const kafka = new Kafka({
  clientId: "erp-mock",
  brokers: [process.env.KAFKA_BROKER || "localhost:19092"],
});
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

async function publish(topic: string, event: BaseEvent & Record<string, unknown>) {
  await producer.send({
    topic,
    messages: [{ key: event.event_id, value: JSON.stringify(event) }],
  });
  console.log(`[ERP] Published ${event.event_type} → ${topic}`);
}

// ── Express app ──

const app = express();
app.use(express.json());

// Healthcheck
app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "erp-mock" });
});

// Capabilities — Platform discovers this system's dimensions automatically
app.get("/api/capabilities", (_req, res) => {
  res.json({
    system_name: "erp",
    system_type: "erp",
    display_name: "ERP Mock",
    data_fields: [
      // Shared dimensions — the shared_dimension field indicates which platform dimension it maps to
      { field_name: "account", field_label: "Account", shared_dimension: "account", role: "producer", label: "Account", taxonomy_type: "account" },
      { field_name: "org_unit", field_label: "Org Unit", shared_dimension: "org_unit", role: "producer", label: "Org Unit", taxonomy_type: "hierarchy" },
      { field_name: "project", field_label: "Project", shared_dimension: "project", role: "producer", label: "Project", taxonomy_type: "flat" },
      // Routing fields — system-specific dimensions that need mapping
      { field_name: "activity", field_label: "Activity", data_type: "string" },
      { field_name: "cost_bearer", field_label: "Cost Center", data_type: "string" },
      { field_name: "counterpart", field_label: "Counterpart", data_type: "string" }
    ]
  });
});

// POST /api/publish-accounts — Step 1 in demo
app.post("/api/publish-accounts", async (_req, res) => {
  const event = {
    event_id: uuid(),
    event_type: "AccountsPublished" as const,
    timestamp: new Date().toISOString(),
    source_system: "erp" as const,
    accounts: [
      // Revenue
      { code: "3010", name: "Service Revenue",  parent: "30", type: "leaf" },
      { code: "3020", name: "Product Sales",    parent: "30", type: "leaf" },
      { code: "3030", name: "Consulting Fees",  parent: "30", type: "leaf" },
      { code: "30",   name: "Revenue",          parent: "RES", type: "group" },
      // Personnel costs
      { code: "4010", name: "Salaries",         parent: "40", type: "leaf" },
      { code: "4020", name: "Social Security",  parent: "40", type: "leaf" },
      { code: "4030", name: "Pension",          parent: "40", type: "leaf" },
      { code: "4040", name: "Consultants",      parent: "40", type: "leaf" },
      { code: "40",   name: "Personnel Costs",  parent: "COSTS", type: "group" },
      // External costs
      { code: "5010", name: "Travel",           parent: "50", type: "leaf" },
      { code: "5020", name: "Marketing",        parent: "50", type: "leaf" },
      { code: "5030", name: "IT Costs",         parent: "50", type: "leaf" },
      { code: "50",   name: "External Costs",   parent: "COSTS", type: "group" },
      // Operating costs
      { code: "6010", name: "Rent",             parent: "60", type: "leaf" },
      { code: "6020", name: "Depreciation",     parent: "60", type: "leaf" },
      { code: "60",   name: "Operating Costs",  parent: "COSTS", type: "group" },
      // Aggregation nodes
      { code: "COSTS", name: "Costs",           parent: "RES", type: "group" },
      { code: "RES",   name: "Result",          parent: null,  type: "group" },
    ],
    org_units: [
      { code: "OU-100", name: "Sales",          parent: "DEPT-A", type: "leaf" },
      { code: "OU-200", name: "Marketing",      parent: "DEPT-A", type: "leaf" },
      { code: "OU-300", name: "IT",             parent: "DEPT-B", type: "leaf" },
      { code: "OU-400", name: "Finance",        parent: "DEPT-B", type: "leaf" },
      { code: "DEPT-A", name: "Marketing & Sales", parent: "ACME", type: "group" },
      { code: "DEPT-B", name: "Operations",     parent: "ACME", type: "group" },
      { code: "ACME",   name: "Acme Inc",       parent: null,  type: "group" },
    ],
  };
  await publish(TOPICS.ERP_ACCOUNTS, event);
  res.json({ ok: true, event });
});

// POST /api/create-project — Step 4 in demo
app.post("/api/create-project", (req, res) => {
  const { name } = req.body;
  const erpId = `erp-${String(Date.now()).slice(-3)}`;
  const event = {
    event_id: uuid(),
    event_type: "ProjectCreated" as const,
    timestamp: new Date().toISOString(),
    source_system: "erp" as const,
    erp_id: erpId,
    name: name || "New ERP Project",
  };
  publish(TOPICS.ERP_PROJECTS, event);
  res.json({ ok: true, event });
});

// ── GL entry generator — full year 2025 with all org units, accounts, varied amounts ──
function generateGlEntries() {
  // Base amounts per account (monthly baseline in SEK)
  const lines: { account: string; org_unit: string; base: number; activity: string; cost_bearer: string; counterpart: string }[] = [
    // Revenue — spread across Sales and Marketing
    { account: "3010", org_unit: "OU-100", base: -820000, activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "3010", org_unit: "OU-200", base: -310000, activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-300" },
    { account: "3020", org_unit: "OU-100", base: -280000, activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "3020", org_unit: "OU-200", base: -190000, activity: "AKT-200", cost_bearer: "KB-600", counterpart: "MP-300" },
    { account: "3030", org_unit: "OU-100", base: -165000, activity: "AKT-200", cost_bearer: "KB-600", counterpart: "MP-200" },
    { account: "3030", org_unit: "OU-300", base: -95000,  activity: "AKT-200", cost_bearer: "KB-600", counterpart: "MP-300" },
    // Personnel costs — IT and Finance
    { account: "4010", org_unit: "OU-100", base: 320000,  activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "4010", org_unit: "OU-200", base: 210000,  activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-300" },
    { account: "4010", org_unit: "OU-300", base: 480000,  activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "4010", org_unit: "OU-400", base: 350000,  activity: "AKT-100", cost_bearer: "KB-600", counterpart: "MP-200" },
    { account: "4020", org_unit: "OU-300", base: 155000,  activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "4020", org_unit: "OU-400", base: 112000,  activity: "AKT-100", cost_bearer: "KB-600", counterpart: "MP-200" },
    { account: "4030", org_unit: "OU-300", base: 78000,   activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "4030", org_unit: "OU-400", base: 98000,   activity: "AKT-100", cost_bearer: "KB-600", counterpart: "MP-200" },
    { account: "4040", org_unit: "OU-100", base: 140000,  activity: "AKT-200", cost_bearer: "KB-600", counterpart: "MP-300" },
    { account: "4040", org_unit: "OU-300", base: 210000,  activity: "AKT-200", cost_bearer: "KB-600", counterpart: "MP-300" },
    // External costs — Sales, Marketing, IT
    { account: "5010", org_unit: "OU-100", base: 45000,   activity: "AKT-200", cost_bearer: "KB-600", counterpart: "MP-300" },
    { account: "5010", org_unit: "OU-200", base: 28000,   activity: "AKT-200", cost_bearer: "KB-600", counterpart: "MP-300" },
    { account: "5020", org_unit: "OU-200", base: 115000,  activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "5020", org_unit: "OU-100", base: 52000,   activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "5030", org_unit: "OU-300", base: 85000,   activity: "AKT-200", cost_bearer: "KB-600", counterpart: "MP-300" },
    { account: "5030", org_unit: "OU-400", base: 32000,   activity: "AKT-200", cost_bearer: "KB-600", counterpart: "MP-300" },
    // Operating costs — Finance
    { account: "6010", org_unit: "OU-400", base: 75000,   activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "6010", org_unit: "OU-300", base: 48000,   activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "6020", org_unit: "OU-300", base: 42000,   activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
    { account: "6020", org_unit: "OU-400", base: 35000,   activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
  ];

  // Seasonal multipliers per month (index 0 = Jan)
  const seasonal = [0.85, 0.90, 0.95, 1.00, 1.05, 1.10, 0.80, 0.60, 1.05, 1.10, 1.15, 1.20];
  // Deterministic pseudo-random variation per line+month
  const vary = (lineIdx: number, month: number) => {
    const seed = (lineIdx * 13 + month * 7 + 37) % 100;
    return 0.92 + (seed / 100) * 0.16; // 0.92–1.08 range
  };

  const entries: Record<string, unknown>[] = [];
  const daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

  for (let m = 1; m <= 12; m++) {
    const period = `2025-${String(m).padStart(2, "0")}`;
    const day = Math.min(10 + m, daysInMonth[m - 1]);
    const txDate = `${period}-${String(day).padStart(2, "0")}`;
    lines.forEach((line, idx) => {
      const amount = Math.round(line.base * seasonal[m - 1] * vary(idx, m));
      entries.push({
        account: line.account,
        org_unit: line.org_unit,
        amount,
        currency: "SEK",
        period,
        transaction_date: txDate,
        activity: line.activity,
        cost_bearer: line.cost_bearer,
        counterpart: line.counterpart,
      });
    });
  }
  return entries;
}

// POST /api/publish-gl — Step 6 in demo
// ERP GL data has richer dimensionality than Product A's budget —
// e.g. "activity" which the budget model lacks. Demonstrates dimension gap.
app.post("/api/publish-gl", (req, res) => {
  const { erp_id, entries } = req.body;
  const event = {
    event_id: uuid(),
    event_type: "GeneralLedgerPublished" as const,
    timestamp: new Date().toISOString(),
    source_system: "erp" as const,
    erp_id: erp_id || "erp-042",
    entries: entries || generateGlEntries(),
  };
  publish(TOPICS.ERP_GENERAL_LEDGER, event);
  res.json({ ok: true, event });
});

// ── Start ──

const PORT = 3001;

async function start() {
  await producer.connect();
  console.log("[ERP] Kafka producer connected");

  app.listen(PORT, () => {
    console.log(`[ERP] Mock running on http://localhost:${PORT}`);
    console.log("[ERP] Endpoints:");
    console.log("  POST /api/publish-accounts  — Publish chart of accounts + org units");
    console.log("  POST /api/create-project    — Create ERP project");
    console.log("  POST /api/publish-gl        — Publish general ledger actuals");
  });
}

start().catch((err) => {
  console.error("[ERP] Failed to start:", err);
  process.exit(1);
});
