import express from "express";
import cors from "cors";
import cookieParser from "cookie-parser";
import jwt from "jsonwebtoken";
import { Kafka } from "kafkajs";
import path from "path";

import { getOrCreateDimensionMapping, getAllDimensionMappings, updateDimensionMapping, configureDimModel, getDimModel, getAllDimModels, configureDimRouting, getAllDimRouting, registerSharedDimension, deleteSharedDimension, getAllSharedDimensions, upsertDimensionCode, getDimensionCodes, deleteDimensionCode, registerParticipant, getParticipants, upsertCodeMapping, getCodeMappings, registerDimensionAttribute, getDimensionAttributes, setCodeAttribute, getCodeAttributes, getAllCodeAttributes, setHierarchy, getHierarchy, resetAllData, getInboxItems, addInboxItem, updateInboxItem, setSystemConfig, getSystemConfig, getAllSystemConfigs, deleteDimModel, deleteDimRouting, deleteSystem, deleteParticipant, getAllUsers, getUser, getUserByUsername, getUserByExternalId, upsertUser, updateUser, deleteUser, updateLastLogin, getUserCount, getAuditEvents, getAuditEventCount, upsertEconEntity, getEconEntities, getEconDimensions, deleteEconEntity, upsertEconEntityAttribute, getEconEntityAttributes, upsertEconAttributeDef, getEconAttributeDefs, upsertEconRelation, getEconRelations, insertEconFacts, validateEconFacts, getEconFacts, getEconFactsSummary, publishEconFacts, getEconFactsForPublish, evaluateErrorPolicy, upsertSyncState, getSyncStates, getSyncState, applyDimRouting, getExternalTools, getAllExternalTools, createExternalTool, updateExternalTool, deleteExternalTool, getDimensionPolicies, upsertDimensionPolicy, deleteDimensionPolicy, applyStructuralPolicies, upsertAttributePublishRule, getAttributePublishRules, deleteAttributePublishRule, deleteEconFactsByPeriods, getEventSubscriptions, setEventSubscription, getDLQItems, getDLQCount, markDLQRetried, insertAuditEvent, computeContentHash, getSyncContentHash, setSyncContentHash, getPipelineHealth, revalidateEconFacts, resetSyncWatermark, getAllHelpArticles, getHelpArticle, getHelpArticleBySlug, searchHelpArticles, getHelpArticlesForUser, createHelpArticle, updateHelpArticle, deleteHelpArticle } from "./mapper";
import { startRouter, publishEntityLinked, publishDimensionSnapshot, getEventLog } from "./router";
import cron from "node-cron";

// ── Config ──

const PORT = 3000;
const JWT_SECRET = "platform-poc-secret-not-for-production";

const DEMO_USERS = [
  { username: "anna", password: "demo", user_id: "user-001", name: "Anna Svensson", email: "anna@example.com", role: "controller", org_unit: "OU-100", products: ["prod_a", "prod_b"], primary_product: "prod_a" },
  { username: "erik", password: "demo", user_id: "user-002", name: "Erik Lindgren", email: "erik@example.com", role: "analyst", org_unit: "OU-200", products: ["prod_b"], primary_product: "prod_b" },
  { username: "calle", password: "demo", user_id: "user-003", name: "Calle Björk", email: "calle@example.com", role: "controller", org_unit: "OU-100", products: ["prod_a"], primary_product: "prod_a" },
  { username: "admin", password: "demo", user_id: "user-000", name: "Admin User", email: "admin@example.com", role: "admin", org_unit: "ACME", products: ["platform", "prod_a", "prod_b"], primary_product: "platform" },
];

// Seed demo users into DB if empty
function seedDemoUsers() {
  if (getUserCount() === 0) {
    for (const u of DEMO_USERS) {
      upsertUser({
        user_id: u.user_id,
        username: u.username,
        name: u.name,
        email: u.email,
        role: u.role,
        org_unit: u.org_unit,
        products: u.products,
        primary_product: u.primary_product,
        source: "local",
        password_hash: u.password,  // plain text for POC — noted as simulated
      });
    }
    console.log("[PLATFORM] Seeded demo users into DB");
  }
}
seedDemoUsers();

// Seed demo help articles
function seedHelpArticles() {
  const existing = getAllHelpArticles();
  if (existing.length === 0) {
    const articles = [
      {
        slug: "getting-started",
        title: "Getting Started with the Platform",
        product: null,
        category: "Platform",
        sort_order: 0,
        keywords: "start, intro, overview, welcome",
        body_md: `# Getting Started

Welcome to the Platform! This guide will help you understand the key concepts.

## What is the Platform?

The platform is a **shared integration layer** that connects your ERP system with downstream products (budgeting, reporting, analysis). It:

- Standardizes data from any ERP into a common format
- Distributes curated data to products via Kafka events
- Provides shared services: inbox, help, identity resolution

## Key Concepts

- **Dimensions** — shared reference data (accounts, org units, projects)
- **Facts** — transactional data (GL entries, budget lines)
- **Events** — notifications when data changes (DimensionSnapshot, GLPublished)
- **Inbox** — aggregated tasks from all products in one place

## Navigation

Use the shell bar at the top to navigate between products. The ? icon opens this help panel, and the sparkle icon opens the AI assistant.`
      },
      {
        slug: "budget-workflow",
        title: "Budget Workflow",
        product: "prod_a",
        category: "Budgeting",
        sort_order: 0,
        keywords: "budget, workflow, approval, submit, draft",
        body_md: `# Budget Workflow

## Creating a Budget

1. Open Product A and navigate to the budget module
2. Select the budget version (e.g. "Budget 2025")
3. Enter amounts per account and period
4. Save as **draft** — this stores locally in Product A

## Submitting a Budget

When your budget is ready:

1. Click **Submit** on the budget version
2. Product A publishes a \`BudgetSubmitted\` event to the platform
3. The platform enriches the data with dimension mappings (planning year, type, version)
4. The enriched budget is forwarded to downstream products (e.g. Product B for consolidation)

## Planning Dimensions

Each budget version is mapped to:
- **Planning Year** — which fiscal year (e.g. 2025)
- **Planning Type** — Budget, Forecast, Outcome
- **Planning Version** — version number within the type

These mappings are configured automatically by the platform but can be adjusted by the platform admin.`
      },
      {
        slug: "gl-data",
        title: "Understanding GL Data Flow",
        product: null,
        category: "Data Pipeline",
        sort_order: 0,
        keywords: "gl, general ledger, transactions, facts, sync",
        body_md: `# GL Data Flow

## How GL data moves through the platform

\`\`\`
ERP → Adapter → Platform (staging) → Validate → Publish → Products
\`\`\`

## Sync Process

1. **Fetch** — The adapter pulls GL entries from the ERP using a high watermark (only new/modified rows)
2. **Stage** — Data is stored in the platform's Economy Domain (\`econ_facts\` table)
3. **Validate** — Each fact is checked: does the account exist? Is the amount valid?
4. **Publish** — Valid facts are published to Kafka (\`platform.gl.out\`)
5. **Consume** — Products receive the event and store in their local \`gl_lines\`

## Deduplication

The pipeline is **idempotent** at every layer:
- ERP provides deterministic entry IDs
- Platform uses UPSERT (same source + row = update, not duplicate)
- Products use period-based replace (delete old period data, insert fresh)

## Period Re-sync

If source data is corrected retroactively, admin can trigger a period re-read:
- Specify period range (e.g. 2025-01 to 2025-03)
- Platform deletes old staged data for those periods and re-fetches from ERP
- Products receive the corrected data with sync_mode = "replace_by_period"`
      },
      {
        slug: "dimensions-explained",
        title: "Shared Dimensions",
        product: null,
        category: "Data Pipeline",
        sort_order: 1,
        keywords: "dimensions, accounts, org units, hierarchy, shared",
        body_md: `# Shared Dimensions

## What are dimensions?

Dimensions are the **reference data** that gives meaning to transactions. Examples:
- **Account** — what type of cost/revenue (e.g. "4010 Salaries")
- **Org Unit** — where in the organization (e.g. "Finance Dept")
- **Project** — which project the cost belongs to

## How they work in the platform

1. The ERP owns the master code list (canonical codes)
2. The platform syncs these and distributes via DimensionSnapshot events
3. Products receive snapshots and maintain local copies
4. Cross-reference mappings handle code translation between systems

## Hierarchies

Dimensions can be hierarchical (org units under departments, accounts grouped by type).
The platform applies **structural policies** to ensure consistent structures:
- Auto Root — adds a single top-level entry point
- Grouping — creates intermediate nodes based on code patterns
- Auto Missing — catches transactions referencing unknown codes`
      },
      {
        slug: "reporting-basics",
        title: "Reporting & Analysis",
        product: "prod_b",
        category: "Reporting",
        sort_order: 0,
        keywords: "report, analysis, product b, consolidation",
        body_md: `# Reporting & Analysis (Product B)

## Overview

Product B receives curated data from the platform and provides:
- GL transaction reporting
- Budget vs. actual comparison
- Dimension-based drill-down

## Data Sources

Product B consumes these platform events:
- **DimensionSnapshot** — accounts, org units, projects with hierarchies
- **GLPublished** — actual transaction data
- **BudgetSubmitted** — budget data with planning dimensions

## Cross-system Identity

When the platform links entities (e.g. ERP project = Product A project), Product B receives an EntityLinked event and can group budget + actuals for the same real-world entity.

## Ingestion Rules

Product B can define rules that derive additional dimensions from member attributes.
For example: "If account has attribute kontoklass=I, tag as Revenue category."`
      },
      {
        slug: "inbox-overview",
        title: "Master Inbox",
        product: null,
        category: "Platform",
        sort_order: 1,
        keywords: "inbox, tasks, notifications, workflow",
        body_md: `# Master Inbox

## What is the Inbox?

The Inbox aggregates tasks from all products into one unified list. When a product needs user action (e.g. "Approve budget", "Review rejected data"), it creates an inbox item.

## How it works

- Products call \`POST /api/inbox\` to create items
- Items appear in the shell bar (bell icon) for the assigned user
- Each item has a deep link back to the source product
- Mark items as done to clear them

## Item Properties

- **Title** — short description of the task
- **Source** — which product created it
- **Priority** — high, medium, low
- **Link** — deep link to the task in the product
- **Category** — type of task (approval, review, action needed)`
      },
      {
        slug: "budget-entry",
        title: "Budget Entry — How to Use",
        product: "prod_a",
        category: "Budgeting",
        sort_order: 1,
        keywords: "budget, entry, grid, edit, amount, period, account",
        body_md: `# Budget Entry

## Overview

The Budget Entry view lets you enter and edit budget amounts per account, org unit, and period. It is the primary data entry surface in Product A.

## How to enter budget data

1. Open **Budget Entry** from the sidebar or the portal home
2. Select the budget version from the tab bar (e.g. "Budget 2025")
3. Each row represents an account; columns represent periods (Jan–Dec)
4. Click a cell to edit the amount — changes are saved automatically
5. Use the dropdowns to filter by org unit or account group

## Toolbar

- **? icon** — Opens this help article (deep-link demo)
- **Refresh icon** — Reloads the page with the latest data from the platform

## Budget statuses

| Status | Meaning |
|--------|---------|
| Draft | In progress, not yet submitted |
| Submitted | Sent to platform for processing |
| Approved | Accepted by the budget coordinator |
| Returned | Sent back for corrections |

## Tips

- Use **Tab** to move between cells quickly
- The grid highlights changed cells in blue until saved
- Negative amounts are shown in red
- Totals update live as you type

## Related

- See **Budget Workflow** for the end-to-end approval process
- See **GL Data Flow** for how actuals compare to your budget`
      }
    ];

    for (const a of articles) {
      createHelpArticle(a);
    }
    console.log("[PLATFORM] Seeded demo help articles");
  }
}
seedHelpArticles();

// ── Kafka ──

const kafka = new Kafka({
  clientId: "platform",
  brokers: [process.env.KAFKA_BROKER || "localhost:19092"],
});

// ── Express ──

const app = express();
app.use(cors({ origin: true, credentials: true }));
app.use(cookieParser());
app.use(express.json());

// ── API Versioning ──
// /api/v1/* is transparently rewritten to /api/* — establishes versioning pattern
// while maintaining backward compat. When v2 is needed, add separate routes.
app.use((req, _res, next) => {
  if (req.path.startsWith("/api/v1/")) {
    req.url = req.url.replace("/api/v1/", "/api/");
  }
  next();
});

app.use(express.static(path.join(__dirname, "..", "public")));

// ── Auth Middleware ──
// Verifies JWT signature on protected routes. Public routes are excluded below.
interface AuthenticatedRequest extends express.Request {
  user?: { user_id: string; name: string; role: string; org_unit: string; products: string[]; primary_product: string };
}

function requireAuth(req: AuthenticatedRequest, res: express.Response, next: express.NextFunction) {
  const token = req.cookies?.platform_token;
  if (!token) {
    res.status(401).json({ error: "Authentication required" });
    return;
  }
  try {
    const payload = jwt.verify(token, JWT_SECRET) as AuthenticatedRequest["user"];
    req.user = payload;
    next();
  } catch {
    res.status(401).json({ error: "Invalid or expired token" });
  }
}

// Apply auth to all /api/* except public endpoints
// Public paths: auth endpoints, SCIM (IdP service account), health, and system-to-system discovery
const PUBLIC_PATHS = ["/api/login", "/api/logout", "/api/scim/v2", "/health", "/api/economy/policies", "/api/economy/dimensions", "/api/help"];

app.use((req: AuthenticatedRequest, res, next) => {
  // Only protect /api/ routes (static files, HTML pages pass through)
  if (!req.path.startsWith("/api/")) return next();
  // Allow public paths
  if (PUBLIC_PATHS.some(p => req.path === p || req.path.startsWith(p + "/"))) return next();
  requireAuth(req, res, next);
});

// ── Auth endpoints ──

app.post("/api/login", (req, res) => {
  const { username, password } = req.body;
  const user = getUserByUsername(username);
  if (!user || user.password_hash !== password) {
    res.status(401).json({ error: "Invalid credentials" });
    return;
  }
  updateLastLogin(user.user_id);
  const token = jwt.sign(
    { user_id: user.user_id, name: user.name, role: user.role, org_unit: user.org_unit, products: user.products, primary_product: user.primary_product },
    JWT_SECRET,
    { expiresIn: "8h" }
  );
  res.cookie("platform_token", token, {
    httpOnly: false, // Shell.js needs to read it
    sameSite: "lax",
    maxAge: 8 * 60 * 60 * 1000,
  });
  res.json({ ok: true, user: { user_id: user.user_id, name: user.name, role: user.role, org_unit: user.org_unit, products: user.products, primary_product: user.primary_product } });
});

app.post("/api/logout", (_req, res) => {
  res.clearCookie("platform_token");
  res.json({ ok: true });
});

app.get("/api/me", (req: AuthenticatedRequest, res) => {
  // Auth middleware has already verified token and set req.user
  res.json(req.user);
});

// Users: from DB (filter by product if ?product=xxx)
app.get("/api/users", (_req, res) => {
  const product = _req.query.product as string | undefined;
  let users = getAllUsers().map(u => ({
    user_id: u.user_id, name: u.name, role: u.role, org_unit: u.org_unit,
    products: u.products, email: u.email, status: u.status, source: u.source,
    groups: u.groups, primary_product: u.primary_product, last_login: u.last_login,
  }));
  if (product) {
    users = users.filter(u => u.products.includes(product));
  }
  res.json(users);
});

// Single user
app.get("/api/users/:id", (req, res) => {
  const user = getUser(req.params.id);
  if (!user) { res.status(404).json({ error: "User not found" }); return; }
  const { password_hash, ...safe } = user;
  res.json(safe);
});

// Create/update user (admin UI)
app.post("/api/users", (req, res) => {
  const { username, name, email, role, org_unit, products, primary_product, password } = req.body;
  if (!username || !name) { res.status(400).json({ error: "username and name required" }); return; }
  const userId = "user-" + String(Date.now()).slice(-6);
  const user = upsertUser({
    user_id: userId,
    username,
    name,
    email: email || undefined,
    role: role || "viewer",
    org_unit: org_unit || undefined,
    products: products || [],
    primary_product: primary_product || undefined,
    source: "local",
    password_hash: password || "demo",
  });
  const { password_hash, ...safe } = user;
  res.status(201).json({ ok: true, user: safe });
});

app.put("/api/users/:id", (req, res) => {
  const result = updateUser(req.params.id, req.body);
  if (!result) { res.status(404).json({ error: "User not found" }); return; }
  const { password_hash, ...safe } = result;
  res.json({ ok: true, user: safe });
});

// Delete user (admin UI)
app.delete("/api/users/:id", (req, res) => {
  const ok = deleteUser(req.params.id);
  if (!ok) { res.status(404).json({ error: "User not found" }); return; }
  res.json({ ok: true });
});

// ── SCIM-like endpoints (simulated) ──
// In production: IdP (Zitadel/Azure AD) pushes user lifecycle events via SCIM 2.0.
// Here we simulate the protocol with simplified JSON payloads.

// Group claim mapping: extracts role:X, product:X, org:X from group names
function parseGroupClaims(groups: string[]): { role?: string; products: string[]; org_unit?: string; primary_product?: string; plainGroups: string[] } {
  const products: string[] = [];
  const plainGroups: string[] = [];
  let role: string | undefined;
  let org_unit: string | undefined;
  for (const g of groups) {
    if (g.startsWith("role:")) { role = g.slice(5); }
    else if (g.startsWith("product:")) { products.push(g.slice(8)); }
    else if (g.startsWith("org:")) { org_unit = g.slice(4); }
    else { plainGroups.push(g); }
  }
  return { role, products, org_unit, primary_product: products[0], plainGroups };
}

app.post("/api/scim/v2/Users", (req, res) => {
  const { externalId, userName, displayName, emails, groups, active, password } = req.body;
  if (!userName || !displayName) {
    res.status(400).json({ error: "userName and displayName required" });
    return;
  }
  const groupNames = groups?.map((g: any) => g.display || g.value) || [];
  const claims = parseGroupClaims(groupNames);
  const userId = "user-" + String(Date.now()).slice(-6);
  const user = upsertUser({
    user_id: userId,
    external_id: externalId || userId,
    username: userName,
    name: displayName,
    email: emails?.[0]?.value || null,
    role: claims.role,
    org_unit: claims.org_unit,
    products: claims.products.length ? claims.products : undefined,
    primary_product: claims.primary_product,
    groups: claims.plainGroups,
    status: active !== false ? "active" : "suspended",
    source: "scim",
    password_hash: password || undefined,
  });
  const { password_hash, ...safe } = user;
  res.status(201).json({ ...safe, schemas: ["urn:ietf:params:scim:schemas:core:2.0:User"] });
});

app.patch("/api/scim/v2/Users/:id", (req, res) => {
  const { Operations } = req.body;
  if (!Operations || !Array.isArray(Operations)) {
    res.status(400).json({ error: "Operations array required" });
    return;
  }
  // Find user by external_id or user_id
  let user = getUserByExternalId(req.params.id) || getUser(req.params.id);
  if (!user) { res.status(404).json({ error: "User not found" }); return; }
  const updates: any = {};
  for (const op of Operations) {
    if (op.op === "replace") {
      if (op.path === "displayName") updates.name = op.value;
      if (op.path === "active") updates.status = op.value ? "active" : "suspended";
      if (op.path === "emails") updates.email = op.value?.[0]?.value;
    }
  }
  const result = updateUser(user.user_id, updates);
  if (!result) { res.status(404).json({ error: "Update failed" }); return; }
  const { password_hash, ...safe } = result;
  res.json({ ...safe, schemas: ["urn:ietf:params:scim:schemas:core:2.0:User"] });
});

app.delete("/api/scim/v2/Users/:id", (req, res) => {
  let user = getUserByExternalId(req.params.id) || getUser(req.params.id);
  if (!user) { res.status(404).json({ error: "User not found" }); return; }
  updateUser(user.user_id, { status: "deprovisioned" });
  res.status(204).end();
});

// ── External Tools CRUD ──
app.get("/api/external-tools", (_req, res) => {
  res.json(getAllExternalTools());
});

app.post("/api/external-tools", (req, res) => {
  const { name, url, icon_url, sort_order } = req.body;
  if (!name || !url) { res.status(400).json({ error: "name and url required" }); return; }
  const tool = createExternalTool({ name, url, icon_url, sort_order });
  res.status(201).json({ ok: true, tool });
});

app.put("/api/external-tools/:id", (req, res) => {
  const result = updateExternalTool(req.params.id, req.body);
  if (!result) { res.status(404).json({ error: "Tool not found" }); return; }
  res.json({ ok: true, tool: result });
});

app.delete("/api/external-tools/:id", (req, res) => {
  const ok = deleteExternalTool(req.params.id);
  if (!ok) { res.status(404).json({ error: "Tool not found" }); return; }
  res.json({ ok: true });
});

// Shell config — lightweight endpoint for shell.js to read display settings
app.get("/api/shell-config", (_req, res) => {
  const useLogo = getSystemConfig("platform", "use_custom_logo");
  res.json({ use_custom_logo: useLogo === "true" });
});

// ── Sync pipeline config (error policy + auto-publish) ──
app.get("/api/economy/pipeline-config/:source", (req, res) => {
  const source = req.params.source;
  res.json({
    error_policy: getSystemConfig(source, "error_policy") || "skip_invalid",
    auto_publish: getSystemConfig(source, "auto_publish") === "true",
  });
});

app.put("/api/economy/pipeline-config/:source", (req, res) => {
  const source = req.params.source;
  const { error_policy, auto_publish } = req.body;
  if (error_policy !== undefined) {
    const valid = ["skip_invalid", "abort_on_error"].includes(error_policy) || error_policy.match(/^threshold:\d+$/);
    if (!valid) { res.status(400).json({ error: "Invalid error_policy. Use: skip_invalid, abort_on_error, or threshold:N" }); return; }
    setSystemConfig(source, "error_policy", error_policy);
  }
  if (auto_publish !== undefined) {
    setSystemConfig(source, "auto_publish", auto_publish ? "true" : "false");
  }
  res.json({ ok: true });
});

// Navigation: dynamic product list based on system config + user entitlements
app.get("/api/navigation", (req, res) => {
  const token = req.cookies?.platform_token;
  if (!token) { res.status(401).json({ error: "Not logged in" }); return; }

  let user: any;
  try { user = jwt.verify(token, JWT_SECRET); } catch { res.status(401).json({ error: "Invalid token" }); return; }

  const allowedProducts = user.products || [];

  // Platform Admin is always available for users who have "platform" in their products
  const items: Array<{ key: string; label: string; url: string }> = [];
  if (allowedProducts.includes("platform")) {
    items.push({ key: "platform", label: "Platform Admin", url: "/admin.html" });
  }

  // Known internal products — use system_config task_base_url for navigation
  const PRODUCTS: Array<{ system_name: string; label: string }> = [
    { system_name: "prod_a", label: "Product A" },
    { system_name: "prod_b", label: "Product B" },
  ];
  for (const p of PRODUCTS) {
    if (!allowedProducts.includes(p.system_name)) continue;
    const taskUrl = getSystemConfig(p.system_name, "task_base_url");
    if (!taskUrl) continue;
    items.push({ key: p.system_name, label: p.label, url: taskUrl });
  }

  // External tools — visible to all users
  const tools = getExternalTools();
  const externalItems = tools.map(t => ({
    key: "ext-" + t.id,
    label: t.name,
    url: t.url,
    external: true,
    icon_url: t.icon_url,
  }));

  res.json({ items, externalTools: externalItems });
});

// ── Platform API ──

// ── Economy Domain: Identity Resolution ──
// The economy domain (not platform infrastructure) owns entity identity.
// When two source entities represent the same real-world entity, the economy domain
// records a "same_as" relation and publishes an EntityLinked event.

app.post("/api/economy/link-entities", async (req, res) => {
  const { dimension, entities } = req.body;
  if (!dimension || !entities || entities.length < 2) {
    res.status(400).json({ error: "dimension and at least 2 entities required" });
    return;
  }
  try {
    // Record same_as relations in economy domain
    const primary = entities[0];
    for (let i = 1; i < entities.length; i++) {
      const other = entities[i];
      upsertEconRelation({
        source_system: "economy_domain",
        relation_type: "same_as",
        dimension,
        child_code: `${other.source_system}:${other.source_key}`,
        parent_code: `${primary.source_system}:${primary.source_key}`,
        hierarchy_name: "identity",
      });
    }
    // Publish EntityLinked event so products can update their local models
    const event = await publishEntityLinked(dimension, entities);
    console.log(`[PLATFORM] Entity linked: ${dimension} — ${entities.map((e: any) => `${e.source_system}:${e.source_key}`).join(" ↔ ")}`);
    res.json({ ok: true, event });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : "Unknown error";
    res.status(400).json({ error: message });
  }
});

// Healthcheck
app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "platform" });
});

// Event log (in-memory ring buffer — fast, volatile)
app.get("/api/events", (req, res) => {
  const limit = Math.min(parseInt(req.query?.limit as string) || 100, 500);
  res.json(getEventLog(limit));
});

// Audit log (persistent — survives restarts)
app.get("/api/audit-events", (req, res) => {
  const limit = Math.min(parseInt(req.query?.limit as string) || 100, 1000);
  res.json({ total: getAuditEventCount(), events: getAuditEvents(limit) });
});

// ── Pipeline Health API ──

app.get("/api/pipeline-health", (_req, res) => {
  res.json(getPipelineHealth());
});

// ── Dead Letter Queue API ──

app.get("/api/dlq", (_req, res) => {
  res.json({ ...getDLQCount(), items: getDLQItems(50) });
});

app.post("/api/dlq/:id/retry", (req, res) => {
  const id = parseInt(req.params.id);
  markDLQRetried(id);
  res.json({ ok: true });
});

// ── Dimension mappings API ──

app.get("/api/dimension-mappings", (_req, res) => {
  res.json(getAllDimensionMappings());
});

app.post("/api/dimension-mappings/configure", (req, res) => {
  const { source_system, source_key, source_version_id, version_name, year } = req.body;
  if (!source_system || !source_key || !version_name || !year) {
    res.status(400).json({ error: "source_system, source_key, version_name and year required" });
    return;
  }
  const mapping = getOrCreateDimensionMapping(source_system, source_key, version_name, year, source_version_id);
  res.json({ ok: true, mapping });
});

app.put("/api/dimension-mappings/:id", (req, res) => {
  const id = parseInt(req.params.id);
  if (isNaN(id)) {
    res.status(400).json({ error: "Invalid mapping ID" });
    return;
  }
  try {
    const updated = updateDimensionMapping(id, req.body);
    res.json({ ok: true, mapping: updated });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    res.status(404).json({ error: message });
  }
});

// ── Flex-dimension model & routing API ──

app.get("/api/dim-model", (_req, res) => {
  res.json(getAllDimModels());
});

app.get("/api/dim-model/:product", (req, res) => {
  res.json(getDimModel(req.params.product));
});

app.post("/api/dim-model", (req, res) => {
  const { product, slot, label } = req.body;
  if (!product || !slot || !label) {
    res.status(400).json({ error: "product, slot and label required" });
    return;
  }
  configureDimModel(product, slot, label);
  res.json({ ok: true, product, slot, label });
});

app.get("/api/dim-routing", (_req, res) => {
  res.json(getAllDimRouting());
});

app.post("/api/dim-routing", (req, res) => {
  const { source_system, source_field, target_product, target_slot } = req.body;
  if (!source_system || !source_field || !target_product || !target_slot) {
    res.status(400).json({ error: "source_system, source_field, target_product and target_slot required" });
    return;
  }
  configureDimRouting(source_system, source_field, target_product, target_slot);
  res.json({ ok: true, source_system, source_field, target_product, target_slot });
});

// ── DELETE endpoints ──

app.delete("/api/dim-model/:product/:slot", (req, res) => {
  deleteDimModel(req.params.product, req.params.slot);
  res.json({ ok: true });
});

app.delete("/api/dim-routing/:source/:field/:target", (req, res) => {
  deleteDimRouting(req.params.source, req.params.field, req.params.target);
  res.json({ ok: true });
});

app.delete("/api/shared-dimensions/:name/participants/:product", (req, res) => {
  deleteParticipant(req.params.name, req.params.product);
  res.json({ ok: true });
});

app.delete("/api/shared-dimensions/:name", (req, res) => {
  deleteSharedDimension(req.params.name);
  res.json({ ok: true });
});

// Bulk-activate all dimensions from Economy Domain
app.post("/api/shared-dimensions/bulk-activate", (_req, res) => {
  const entities = getEconEntities();
  const byDim: Record<string, { count: number; source: string }> = {};
  for (const e of entities) {
    if (!byDim[e.dimension]) byDim[e.dimension] = { count: 0, source: e.source_system };
    byDim[e.dimension].count++;
  }
  const existing = getAllSharedDimensions().map(d => d.name);
  const activated: string[] = [];
  const dimTypeMap: Record<string, string> = { account: "account", org_unit: "hierarchy", project: "flat" };
  for (const [dim, info] of Object.entries(byDim)) {
    if (existing.includes(dim)) continue;
    const label = dim.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
    const dimType = dimTypeMap[dim] || "flat";
    registerSharedDimension(dim, label, info.source || "erp", "shared", dimType);
    activated.push(dim);
  }
  res.json({ ok: true, activated, skipped: existing });
});

// ── Shared Dimension Catalog API ──

app.get("/api/shared-dimensions", (_req, res) => {
  res.json(getAllSharedDimensions());
});

app.post("/api/shared-dimensions", (req, res) => {
  const { name, label, owner_system, taxonomy_type } = req.body;
  if (!name || !label || !owner_system) {
    res.status(400).json({ error: "name, label and owner_system required" });
    return;
  }
  registerSharedDimension(name, label, owner_system, taxonomy_type || "shared");
  res.json({ ok: true, name, label, owner_system, taxonomy_type: taxonomy_type || "shared" });
});

app.get("/api/shared-dimensions/:name/codes", (req, res) => {
  res.json(getDimensionCodes(req.params.name));
});

app.post("/api/shared-dimensions/:name/codes", (req, res) => {
  const { code, label } = req.body;
  if (!code || !label) {
    res.status(400).json({ error: "code and label required" });
    return;
  }
  upsertDimensionCode(req.params.name, code, label);
  res.json({ ok: true, dimension: req.params.name, code, label });
});

app.delete("/api/shared-dimensions/:name/codes/:code", (req, res) => {
  deleteDimensionCode(req.params.name, req.params.code);
  res.json({ ok: true, dimension: req.params.name, code: req.params.code });
});

app.patch("/api/shared-dimensions/:name/codes/:code", (req, res) => {
  const { label } = req.body;
  if (!label) { res.status(400).json({ error: "label required" }); return; }
  upsertDimensionCode(req.params.name, req.params.code, label);
  res.json({ ok: true, dimension: req.params.name, code: req.params.code, label });
});

app.post("/api/shared-dimensions/:name/participants", (req, res) => {
  const { product, role, uses_canonical } = req.body;
  if (!product || !role) {
    res.status(400).json({ error: "product and role required" });
    return;
  }
  registerParticipant(req.params.name, product, role, uses_canonical !== false);
  res.json({ ok: true, dimension: req.params.name, product, role });
});

app.get("/api/shared-dimensions/:name/mappings", (req, res) => {
  const product = req.query.product as string | undefined;
  res.json(getCodeMappings(req.params.name, product));
});

app.post("/api/shared-dimensions/:name/mappings", (req, res) => {
  const { product, local_code, canonical_code, source_key } = req.body;
  if (!product || !local_code || !canonical_code) {
    res.status(400).json({ error: "product, local_code and canonical_code required" });
    return;
  }
  upsertCodeMapping(req.params.name, product, local_code, canonical_code, source_key);
  res.json({ ok: true, dimension: req.params.name, product, local_code, canonical_code, source_key: source_key || null });
});

// Unified member view: canonical codes + per-system cross-references + attributes
app.get("/api/shared-dimensions/:name/members", (req, res) => {
  const name = req.params.name;
  const codes = getDimensionCodes(name);
  const mappings = getCodeMappings(name);
  const codeAttrs = getAllCodeAttributes(name);
  const attrDefs = getDimensionAttributes(name);

  // Build attribute map: code → {attr: value}
  const attrMap: Record<string, Record<string, string>> = {};
  for (const ca of codeAttrs as any[]) {
    if (!attrMap[ca.code]) attrMap[ca.code] = {};
    attrMap[ca.code][ca.attribute_name] = ca.value;
  }

  // Build xref map: code → [{product, local_code, source_key}]
  const xrefMap: Record<string, any[]> = {};
  for (const m of mappings as any[]) {
    if (!xrefMap[m.canonical_code]) xrefMap[m.canonical_code] = [];
    xrefMap[m.canonical_code].push({ product: m.product, local_code: m.local_code, source_key: m.source_key || null });
  }

  // Get participants
  const participants = getParticipants(name);

  const members = (codes as any[]).map(c => ({
    code: c.code,
    label: c.label,
    attributes: attrMap[c.code] || {},
    xrefs: xrefMap[c.code] || [],
  }));

  res.json({ dimension: name, attribute_definitions: attrDefs, participants, members });
});

// ── Dimension Attributes & Hierarchy API ──

app.get("/api/shared-dimensions/:name/attributes", (req, res) => {
  res.json(getDimensionAttributes(req.params.name));
});

app.post("/api/shared-dimensions/:name/attributes", (req, res) => {
  const { attribute_name, attribute_label, data_type } = req.body;
  if (!attribute_name || !attribute_label) {
    res.status(400).json({ error: "attribute_name and attribute_label required" });
    return;
  }
  registerDimensionAttribute(req.params.name, attribute_name, attribute_label, data_type || "string");
  res.json({ ok: true, dimension: req.params.name, attribute_name, attribute_label });
});

app.get("/api/shared-dimensions/:name/codes/:code/attributes", (req, res) => {
  res.json(getCodeAttributes(req.params.name, req.params.code));
});

app.post("/api/shared-dimensions/:name/codes/:code/attributes", (req, res) => {
  const { attribute_name, value } = req.body;
  if (!attribute_name || value === undefined) {
    res.status(400).json({ error: "attribute_name and value required" });
    return;
  }
  setCodeAttribute(req.params.name, req.params.code, attribute_name, value);
  res.json({ ok: true, dimension: req.params.name, code: req.params.code, attribute_name, value });
});

app.get("/api/shared-dimensions/:name/code-attributes", (req, res) => {
  res.json(getAllCodeAttributes(req.params.name));
});

app.get("/api/shared-dimensions/:name/hierarchy", (req, res) => {
  res.json(getHierarchy(req.params.name));
});

app.post("/api/shared-dimensions/:name/hierarchy", (req, res) => {
  const { child_code, parent_code, level } = req.body;
  if (!child_code || !parent_code) {
    res.status(400).json({ error: "child_code and parent_code required" });
    return;
  }
  setHierarchy(req.params.name, child_code, parent_code, level || 0);
  res.json({ ok: true, dimension: req.params.name, child_code, parent_code, level: level || 0 });
});

// ── Inbox ──

app.get("/api/inbox", (req, res) => {
  // Resolve user from JWT if present
  let username: string | undefined;
  try {
    const token = req.cookies?.platform_token;
    if (token) {
      const payload = jwt.verify(token, JWT_SECRET) as any;
      username = payload.user_id;
    }
  } catch { /* ignore */ }

  const status = req.query.status as string | undefined;
  const items = getInboxItems(username, status);

  // Resolve deep links: task_base_url + task_path → full link
  const FALLBACK_URLS: Record<string, string> = { prod_a: "http://localhost:3002", prod_b: "http://localhost:3003" };
  const enriched = items.map((item: any) => {
    let resolved_link = item.link;
    if (!resolved_link && item.task_path && item.source) {
      const baseUrl = item.source === "platform" ? `http://localhost:${PORT}` : (getSystemConfig(item.source, "task_base_url") || FALLBACK_URLS[item.source]);
      if (baseUrl) resolved_link = baseUrl + item.task_path;
    }
    return { ...item, resolved_link };
  });

  res.json(enriched);
});

app.patch("/api/inbox/:id", (req, res) => {
  const { status } = req.body;
  if (!status) { res.status(400).json({ error: "status required" }); return; }
  updateInboxItem(req.params.id, { status });
  res.json({ ok: true });
});

// ── System Config API ──

app.get("/api/system-config", (_req, res) => {
  // Group by system_name for easier consumption
  const rows = getAllSystemConfigs();
  const grouped: Record<string, Record<string, string>> = {};
  for (const r of rows) {
    if (!grouped[r.system_name]) grouped[r.system_name] = {};
    grouped[r.system_name][r.config_key] = r.config_value;
  }
  res.json(grouped);
});

app.put("/api/system-config/:system", (req, res) => {
  const system = req.params.system;
  const entries = req.body;
  if (!entries || typeof entries !== "object") { res.status(400).json({ error: "body must be key-value object" }); return; }
  for (const [key, value] of Object.entries(entries)) {
    setSystemConfig(system, key, value as string);
  }
  res.json({ ok: true });
});

app.delete("/api/systems/:system", (req, res) => {
  deleteSystem(req.params.system);
  res.json({ ok: true });
});

// ── Event Subscriptions ──

app.get("/api/subscriptions", (_req, res) => {
  res.json(getEventSubscriptions());
});

app.put("/api/subscriptions", (req, res) => {
  const { product, event_type, enabled } = req.body;
  if (!product || !event_type) { res.status(400).json({ error: "product and event_type required" }); return; }
  setEventSubscription(product, event_type, !!enabled);
  res.json({ ok: true });
});

// ── Economy Domain API ──

// Dimensions (distinct dimensions from econ_entities)
app.get("/api/economy/dimensions", (_req, res) => {
  res.json(getEconDimensions());
});

// Entities (dimension members)
app.get("/api/economy/entities", (req, res) => {
  const dimension = req.query.dimension as string | undefined;
  res.json(getEconEntities(dimension));
});

app.post("/api/economy/entities", (req, res) => {
  const items = Array.isArray(req.body) ? req.body : [req.body];
  for (const item of items) {
    if (!item.source_system || !item.dimension || !item.code || !item.name) {
      res.status(400).json({ error: "source_system, dimension, code, name required" }); return;
    }
    upsertEconEntity(item);
  }
  res.json({ ok: true, upserted: items.length });
});

app.delete("/api/economy/entities/:dimension/:code", (req, res) => {
  const ok = deleteEconEntity(req.params.dimension, req.params.code);
  res.json({ ok, deleted: ok ? 1 : 0 });
});

// Entity attributes
app.get("/api/economy/entities/:dimension/:code/attributes", (req, res) => {
  res.json(getEconEntityAttributes(req.params.dimension, req.params.code));
});

app.post("/api/economy/entities/:dimension/:code/attributes", (req, res) => {
  const items = Array.isArray(req.body) ? req.body : [req.body];
  for (const item of items) {
    upsertEconEntityAttribute({ dimension: req.params.dimension, code: req.params.code, ...item });
  }
  res.json({ ok: true, upserted: items.length });
});

// Attribute definitions
app.get("/api/economy/attribute-defs", (req, res) => {
  const dimension = req.query.dimension as string | undefined;
  res.json(getEconAttributeDefs(dimension));
});

app.post("/api/economy/attribute-defs", (req, res) => {
  const items = Array.isArray(req.body) ? req.body : [req.body];
  for (const item of items) { upsertEconAttributeDef(item); }
  res.json({ ok: true, upserted: items.length });
});

// Attribute Publish Rules
app.get("/api/economy/attribute-publish-rules", (req, res) => {
  const dimension = req.query.dimension as string | undefined;
  res.json(getAttributePublishRules(dimension));
});

app.post("/api/economy/attribute-publish-rules", (req, res) => {
  const { dimension, source_attribute, publish_as, transform, enabled } = req.body;
  if (!dimension || !source_attribute || !publish_as) {
    res.status(400).json({ error: "dimension, source_attribute, publish_as required" }); return;
  }
  upsertAttributePublishRule({ dimension, source_attribute, publish_as, transform: transform ? JSON.stringify(transform) : undefined, enabled: enabled ?? 1 });
  res.json({ ok: true });
});

app.delete("/api/economy/attribute-publish-rules/:id", (req, res) => {
  const ok = deleteAttributePublishRule(parseInt(req.params.id));
  res.json({ ok });
});

// Relations (hierarchies)
app.get("/api/economy/relations", (req, res) => {
  const { dimension, hierarchy } = req.query as { dimension?: string; hierarchy?: string };
  res.json(getEconRelations(dimension, hierarchy));
});

app.post("/api/economy/relations", (req, res) => {
  const items = Array.isArray(req.body) ? req.body : [req.body];
  for (const item of items) {
    if (!item.source_system || !item.dimension || !item.child_code || !item.parent_code) {
      res.status(400).json({ error: "source_system, dimension, child_code, parent_code required" }); return;
    }
    upsertEconRelation(item);
  }
  res.json({ ok: true, upserted: items.length });
});

// Facts (GL staging)
app.get("/api/economy/facts", (req, res) => {
  const { status, project_id, limit } = req.query as any;
  res.json(getEconFacts({ status, project_id, limit: limit ? parseInt(limit) : undefined }));
});

app.get("/api/economy/facts/summary", (_req, res) => {
  res.json(getEconFactsSummary());
});

app.post("/api/economy/facts", (req, res) => {
  const items = Array.isArray(req.body) ? req.body : req.body.facts;
  if (!items?.length) { res.status(400).json({ error: "Array of facts required" }); return; }
  const result = insertEconFacts(items);
  res.json(result);
});

app.post("/api/economy/facts/validate", (req, res) => {
  const batchId = req.body.batch_id;
  const result = validateEconFacts(batchId);
  // Also report how many are already validated (ready for publish)
  const summary = getEconFactsSummary();
  res.json({ ...result, already_validated: summary.validated });
});

// ── Reusable fact publish logic ──
async function executeFactPublish(): Promise<{ published: number }> {
  const facts = getEconFactsForPublish();
  if (facts.length === 0) return { published: 0 };

  const producer = kafka.producer();
  await producer.connect();

  // 1. Internal economy topic
  await producer.send({
    topic: "economy.facts.published",
    messages: [{ key: "publish", value: JSON.stringify({ facts, published_at: new Date().toISOString() }) }],
  });

  // 2. Route to platform.gl.out grouped by project so Product B can consume
  const byProject = new Map<string, any[]>();
  for (const f of facts) {
    const key = f.project_id || "_no_project";
    if (!byProject.has(key)) byProject.set(key, []);
    byProject.get(key)!.push(f);
  }
  for (const [projectId, entries] of byProject) {
    await producer.send({
      topic: "platform.projects.out",
      messages: [{ key: projectId, value: JSON.stringify({ source_system: "erp", source_key: projectId, name: projectId }) }],
    });
    const glEntries = entries.map(e => ({
      account: e.account, org_unit: e.org_unit, amount: e.amount,
      currency: e.currency || "SEK", period: e.period,
      transaction_date: e.transaction_date || null,
      dim1: e.dim1, dim2: e.dim2, dim3: e.dim3,
    }));
    const dimValuesPerEntry = entries.map(e => {
      const sourceData: Record<string, unknown> = {
        activity: e.dim1, cost_bearer: e.dim2, counterpart: e.dim3,
        dim1: e.dim1, dim2: e.dim2, dim3: e.dim3,
      };
      return applyDimRouting(entries[0].source_system || "erp", "prod_b", sourceData);
    });
    const batchPeriods = [...new Set(entries.map((e: any) => e.period))].sort();
    await producer.send({
      topic: "platform.gl.out",
      messages: [{ key: projectId, value: JSON.stringify({
        source_system: entries[0].source_system || "erp",
        source_key: projectId,
        sync_mode: "replace_by_period",
        periods: batchPeriods,
        dim_values_per_entry: dimValuesPerEntry,
        original: { erp_id: projectId, entries: glEntries },
      }) }],
    });
  }

  await producer.disconnect();
  const count = publishEconFacts();
  return { published: count };
}

app.post("/api/economy/facts/publish", async (_req, res) => {
  try {
    const result = await executeFactPublish();
    res.json(result);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Sync state
app.get("/api/economy/sync-state", (_req, res) => {
  res.json(getSyncStates());
});

app.get("/api/economy/sync-state/:source/:entityType", (req, res) => {
  const state = getSyncState(req.params.source, req.params.entityType);
  res.json(state || { error: "not found" });
});

app.post("/api/economy/sync-state", (req, res) => {
  const { source_system, entity_type, ...updates } = req.body;
  if (!source_system || !entity_type) { res.status(400).json({ error: "source_system, entity_type required" }); return; }
  upsertSyncState(source_system, entity_type, updates);
  res.json({ ok: true });
});

// Dimension Policies
app.get("/api/economy/policies", (req, res) => {
  const dimension = req.query.dimension as string | undefined;
  res.json(getDimensionPolicies(dimension));
});

app.post("/api/economy/policies", (req, res) => {
  const { dimension, policy_type, config, enabled } = req.body;
  if (!dimension || !policy_type) { res.status(400).json({ error: "dimension, policy_type required" }); return; }
  upsertDimensionPolicy(dimension, policy_type, config || {}, enabled ?? 1);
  res.json({ ok: true });
});

app.delete("/api/economy/policies/:dimension/:policyType", (req, res) => {
  const ok = deleteDimensionPolicy(req.params.dimension, req.params.policyType);
  res.json({ ok });
});

app.post("/api/economy/policies/apply", async (req, res) => {
  const { dimension } = req.body;
  if (!dimension) { res.status(400).json({ error: "dimension required" }); return; }
  const result = applyStructuralPolicies(dimension);
  // Publish dimension snapshot to downstream products
  if (result.applied > 0) {
    try {
      await publishDimensionSnapshot(dimension, getEconEntities(dimension), getEconRelations(dimension), getEconAttributeDefs(dimension), getEconEntityAttributes(dimension));
    } catch (e) { console.error("[POLICIES] Failed to publish dimension snapshot:", e); }
  }
  res.json(result);
});

app.post("/api/economy/policies/apply-all", async (_req, res) => {
  const dims = (getEconEntities() as any[]).reduce((acc: Set<string>, e: any) => { acc.add(e.dimension); return acc; }, new Set<string>());
  const results: Record<string, any> = {};
  for (const dim of dims) {
    results[dim] = applyStructuralPolicies(dim);
  }
  // Publish dimension snapshots for every dimension that had policies applied
  for (const dim of dims) {
    if (results[dim]?.applied > 0) {
      try {
        await publishDimensionSnapshot(dim as string, getEconEntities(dim as string), getEconRelations(dim as string), getEconAttributeDefs(dim as string), getEconEntityAttributes(dim as string));
      } catch (e) { console.error(`[POLICIES] Failed to publish ${dim} snapshot:`, e); }
    }
  }
  res.json({ results, dimensions: [...dims] });
});

// ── Economy Scheduler ──

const scheduledJobs: Map<string, cron.ScheduledTask> = new Map();

// Sync runner: fetches data from ERP and stages into economy domain
// scope: "all" = entities+facts, "entities" = structure only, "facts" = transactions only
async function runEconSync(source: string, scope: "all" | "entities" | "facts" = "all", req?: any) {
  const t0 = Date.now();
  console.log(`[SCHEDULER] Starting economy sync for ${source} (scope: ${scope})...`);
  try {
    if (scope !== "facts") upsertSyncState(source, "entities", { status: "syncing" });
    if (scope !== "entities") upsertSyncState(source, "facts", { status: "syncing" });

    // 1. Sync entities (accounts + org_units)
    let entityCount = 0, relCount = 0;
   if (scope !== "facts") {
    // Discover attribute metadata from ERP capabilities (self-describing API)
    const capResp = await fetch(`${ERP_URL}/api/capabilities`);
    const capabilities = await capResp.json() as any;
    const memberAttrDefs: Record<string, Array<{ attribute_name: string; attribute_label: string; data_type?: string; allowed_values?: string[] }>> = capabilities.member_attributes || {};

    // Register attribute definitions from ERP
    const knownAttrs: Record<string, string[]> = {}; // dimension → [attr_name, ...]
    for (const [dim, defs] of Object.entries(memberAttrDefs)) {
      knownAttrs[dim] = [];
      for (const def of defs as any[]) {
        upsertEconAttributeDef({
          dimension: dim,
          attribute_name: def.attribute_name,
          attribute_label: def.attribute_label,
          data_type: def.data_type || "string",
          source_system: source,
          allowed_values: def.allowed_values ? JSON.stringify(def.allowed_values) : undefined,
        });
        knownAttrs[dim].push(def.attribute_name);
      }
    }

    const accResp = await fetch(`${ERP_URL}/api/publish-accounts`, { method: "POST" });
    const accData = await accResp.json() as any;
    const accounts = accData.event?.accounts || accData.accounts || [];
    const orgUnits = accData.event?.org_units || accData.org_units || [];

    // Helper: extract attribute values from a member object based on known attribute names
    function extractAttributes(member: Record<string, any>, dimension: string) {
      const attrNames = knownAttrs[dimension] || [];
      for (const attr of attrNames) {
        if (member[attr] != null) {
          upsertEconEntityAttribute({ dimension, code: member.code, attribute_name: attr, attribute_value: String(member[attr]), source_system: source });
        }
      }
    }

    for (const acc of accounts) {
      upsertEconEntity({ source_system: source, dimension: "account", code: acc.code, name: acc.name, type: acc.type || "leaf" });
      entityCount++;
      if (acc.parent) { upsertEconRelation({ source_system: source, dimension: "account", child_code: acc.code, parent_code: acc.parent, hierarchy_name: "standard" }); relCount++; }
      extractAttributes(acc, "account");
    }
    for (const org of orgUnits) {
      upsertEconEntity({ source_system: source, dimension: "org_unit", code: org.code, name: org.name, type: org.type || "leaf" });
      entityCount++;
      if (org.parent) { upsertEconRelation({ source_system: source, dimension: "org_unit", child_code: org.code, parent_code: org.parent, hierarchy_name: "standard" }); relCount++; }
      extractAttributes(org, "org_unit");
    }

    // Extra hierarchy nodes
    upsertEconEntity({ source_system: source, dimension: "org_unit", code: "DIV-01", name: "Division South", type: "group" });
    entityCount++;
    upsertEconRelation({ source_system: source, dimension: "org_unit", child_code: "OU-100", parent_code: "DIV-01", hierarchy_name: "standard", level: 1 }); relCount++;
    upsertEconRelation({ source_system: source, dimension: "org_unit", child_code: "OU-200", parent_code: "DIV-01", hierarchy_name: "standard", level: 1 }); relCount++;

    // Flex dimensions use platform dim-slots (dim1–dim5), not ERP-specific names.
    // The adapter maps ERP fields to slots; dim_routing tells products what each slot means.
    const flexDims = [
      { dim: "dim1", codes: [
        { code: "AKT-100", name: "Design" },
        { code: "AKT-200", name: "Construction" },
        { code: "AKT-300", name: "Inspection" },
      ]},
      { dim: "dim2", codes: [
        { code: "KB-500", name: "Internal" },
        { code: "KB-600", name: "External" },
      ]},
      { dim: "dim3", codes: [
        { code: "MP-200", name: "Supplier Alpha" },
        { code: "MP-300", name: "Supplier Beta" },
      ]},
    ];
    for (const fd of flexDims) {
      for (const c of fd.codes) {
        upsertEconEntity({ source_system: source, dimension: fd.dim, code: c.code, name: c.name, type: "leaf" });
        entityCount++;
      }
    }

    // Register "project" as a known dimension (projects are created individually later)
    upsertEconEntity({ source_system: source, dimension: "project", code: "_placeholder", name: "(projects added dynamically)", type: "system" });
    entityCount++;

    upsertSyncState(source, "entities", { last_sync_at: new Date().toISOString(), rows_received: entityCount, rows_validated: entityCount, status: "idle", duration_ms: Date.now() - t0 });
    upsertSyncState(source, "relations", { last_sync_at: new Date().toISOString(), rows_received: relCount, rows_validated: relCount, status: "idle" });
    insertAuditEvent("in", `economy.sync.entities`, "EntitiesSync", undefined, source, `Synced ${entityCount} entities, ${relCount} relations from ${source}`);

    // Apply structural policies to all synced dimensions (dynamic — reads from DB)
    const syncedDims = getEconDimensions().map(d => d.dimension);
    for (const dim of syncedDims) {
      const result = applyStructuralPolicies(dim);
      if (result.applied > 0) console.log(`[SCHEDULER] Structural policies for ${dim}: ${result.applied} applied, ${result.created_entities} entities created, ${result.created_relations} relations created`);
    }
    // Publish dimension snapshots to downstream products (with change detection)
    for (const dim of syncedDims) {
      try {
        const entities = getEconEntities(dim);
        const relations = getEconRelations(dim);
        const attrDefs = getEconAttributeDefs(dim);
        const attrs = getEconEntityAttributes(dim);
        // Compute content hash to detect changes
        const hash = computeContentHash({ entities, relations, attrs });
        const prevHash = getSyncContentHash(source, `dim:${dim}`);
        if (hash === prevHash) {
          console.log(`[SCHEDULER] Dimension ${dim}: unchanged (hash ${hash}), skipping publish`);
          continue;
        }
        await publishDimensionSnapshot(dim, entities, relations, attrDefs, attrs);
        setSyncContentHash(source, `dim:${dim}`, hash);
        console.log(`[SCHEDULER] Dimension ${dim}: changed (${prevHash || 'new'} → ${hash}), published`);
        insertAuditEvent("out", `economy.${dim}.snapshot`, "DimensionPublished", undefined, source, `Published ${dim} snapshot (${entities.length} entities, hash: ${hash})`);
      } catch (e) { console.error(`[SCHEDULER] Failed to publish ${dim} snapshot:`, e); }
    }
   } // end scope !== "facts"

    // 2. Sync facts — pull from ERP with period filtering and idempotent upsert
   if (scope !== "entities") {
    const syncState = getSyncState(source, "facts") as any;
    const highWatermark = syncState?.high_watermark || null;

    // Build query params for incremental or period-based sync
    const params = new URLSearchParams();
    if ((req as any)?.query?.period_from) params.set("period_from", (req as any).query.period_from);
    if ((req as any)?.query?.period_to) params.set("period_to", (req as any).query.period_to);
    if (!params.has("period_from") && highWatermark) params.set("modified_since", highWatermark);

    const qs = params.toString() ? `?${params.toString()}` : "";
    const glResp = await fetch(`${ERP_URL}/api/gl${qs}`);
    const glData = await glResp.json() as any;
    const entries: any[] = glData.entries || [];
    const periods: string[] = glData.periods || [];

    // If explicit period range requested, delete old facts first (handles source-side deletes)
    const isPeriodSync = params.has("period_from") || params.has("period_to");
    if (isPeriodSync && periods.length > 0) {
      deleteEconFactsByPeriods(source, periods);
      console.log(`[SCHEDULER] Deleted old facts for periods: ${periods.join(",")}`);
    }

    if (entries.length > 0) {
      const batchId = `sync-${source}-${Date.now()}`;
      const projectId = demoState.erp_id || "ERP-GL";

      const facts = entries.map((e: any) => ({
        source_system: source,
        source_batch_id: batchId,
        source_row_id: e.entry_id || null,
        source_modified_at: e.modified_at || null,
        project_id: projectId,
        account: e.account,
        org_unit: e.org_unit,
        period: e.period,
        amount: e.amount,
        currency: e.currency || "SEK",
        transaction_date: e.transaction_date,
        dim1: e.activity,
        dim2: e.cost_bearer,
        dim3: e.counterpart,
      }));

      const ins = insertEconFacts(facts);
      const val = validateEconFacts(ins.batch_id);
      const newWatermark = glData.high_watermark || highWatermark;

      // ── Auto-publish based on error policy ──
      const errorPolicy = getSystemConfig(source, "error_policy") || "skip_invalid";
      const autoPublish = getSystemConfig(source, "auto_publish") === "true";
      const policyResult = evaluateErrorPolicy(errorPolicy, val.validated, val.rejected);
      let publishedCount = 0;

      if (autoPublish && val.validated > 0) {
        if (policyResult.allowed) {
          try {
            const pubResult = await executeFactPublish();
            publishedCount = pubResult.published;
            console.log(`[SCHEDULER] Auto-publish: ${publishedCount} facts published (policy: ${errorPolicy})`);
          } catch (e: any) {
            console.error(`[SCHEDULER] Auto-publish failed:`, e.message);
          }
        } else {
          console.warn(`[SCHEDULER] Auto-publish BLOCKED by policy: ${policyResult.reason}`);
          insertAuditEvent("out", `economy.publish.blocked`, "PublishBlocked", undefined, source, `Auto-publish blocked: ${policyResult.reason} (${val.rejected} rejected of ${val.validated + val.rejected})`);
        }
      }

      upsertSyncState(source, "facts", {
        last_sync_at: new Date().toISOString(),
        high_watermark: newWatermark || undefined,
        rows_received: ins.received + ins.updated,
        rows_validated: val.validated,
        rows_rejected: val.rejected,
        status: policyResult.allowed ? "idle" : "blocked",
        duration_ms: Date.now() - t0,
      });
      console.log(`[SCHEDULER] Facts sync: ${ins.received} new, ${ins.updated} updated, ${val.validated} validated, ${val.rejected} rejected, ${publishedCount} published (policy: ${errorPolicy}, auto: ${autoPublish}) (periods: ${periods.join(",")})`);
      insertAuditEvent("in", `economy.sync.facts`, "FactsSync", undefined, source, `Synced ${ins.received} new + ${ins.updated} updated, ${val.validated} valid, ${val.rejected} rejected, ${publishedCount} published (policy: ${errorPolicy})`);
    } else {
      upsertSyncState(source, "facts", { status: "idle", duration_ms: Date.now() - t0 });
    }
   } // end scope !== "entities"

    console.log(`[SCHEDULER] Economy sync for ${source} (${scope}) done in ${Date.now() - t0}ms — ${entityCount} entities, ${relCount} relations`);
  } catch (err: any) {
    console.error(`[SCHEDULER] Economy sync for ${source} failed:`, err.message);
    upsertSyncState(source, "entities", { status: "error" });
    upsertSyncState(source, "facts", { status: "error" });
  }
}

// Schedule or reschedule a sync job
function scheduleEconSync(source: string, cronExpr: string) {
  const key = `econ-${source}`;
  const existing = scheduledJobs.get(key);
  if (existing) { existing.stop(); scheduledJobs.delete(key); }
  if (!cron.validate(cronExpr)) return false;
  const task = cron.schedule(cronExpr, () => runEconSync(source));
  scheduledJobs.set(key, task);
  upsertSyncState(source, "entities", { schedule_cron: cronExpr });
  upsertSyncState(source, "facts", { schedule_cron: cronExpr });
  console.log(`[SCHEDULER] Scheduled ${source} economy sync: ${cronExpr}`);
  return true;
}

// API: Manual sync trigger
app.post("/api/economy/sync/:source/run", async (req, res) => {
  const scope = (req.query.scope as string) || "all";
  if (!["all", "entities", "facts"].includes(scope)) { res.status(400).json({ error: "scope must be all, entities, or facts" }); return; }
  await runEconSync(req.params.source, scope as any, req);
  res.json({ ok: true, source: req.params.source, scope });
});

// API: Schedule sync
app.post("/api/economy/sync/:source/schedule", (req, res) => {
  const { cron: cronExpr } = req.body;
  if (!cronExpr) { res.status(400).json({ error: "cron expression required" }); return; }
  const ok = scheduleEconSync(req.params.source, cronExpr);
  if (!ok) { res.status(400).json({ error: "Invalid cron expression" }); return; }
  res.json({ ok: true, source: req.params.source, cron: cronExpr });
});

// API: List scheduled jobs
app.get("/api/economy/scheduler", (_req, res) => {
  const jobs = Array.from(scheduledJobs.entries()).map(([key, task]) => ({
    key, running: (task as any).options?.scheduled !== false,
  }));
  res.json(jobs);
});

// API: Stop scheduled job
app.delete("/api/economy/sync/:source/schedule", (req, res) => {
  const key = `econ-${req.params.source}`;
  const existing = scheduledJobs.get(key);
  if (existing) { existing.stop(); scheduledJobs.delete(key); }
  res.json({ ok: true, stopped: !!existing });
});

// API: Re-validate (reset rejected → received, then validate again)
app.post("/api/economy/facts/revalidate", (_req, res) => {
  const result = revalidateEconFacts();
  insertAuditEvent("in", "economy.revalidate", "Revalidate", undefined, undefined, `Re-validated: ${result.reset} reset, ${result.validated} validated, ${result.rejected} still rejected`);
  res.json({ ok: true, ...result });
});

// API: Full re-read (reset watermark + trigger sync)
app.post("/api/economy/sync/:source/full-reread", async (req, res) => {
  resetSyncWatermark(req.params.source, "facts");
  insertAuditEvent("in", "economy.full-reread", "FullReread", undefined, req.params.source, `Watermark reset for ${req.params.source} — starting full re-read`);
  await runEconSync(req.params.source, "facts");
  res.json({ ok: true, description: `Full re-read completed for ${req.params.source}` });
});

// ── Demo Runner ──
// Orchestrates the 8-step demo from Platform by calling ERP/Product A/Product B APIs

const ERP_URL = process.env.ERP_URL || "http://erp-mock:3001";
const PRODUCT_A_URL = process.env.PRODUCT_A_URL || "http://product-a:3002";
const PRODUCT_B_URL = process.env.PRODUCT_B_URL || "http://product-b:3003";

const demoState = {
  prod_a_id: null as string | null,
  erp_id: null as string | null,
  version_id: null as string | null,
  step: 0,
};

app.get("/api/demo/state", (_req, res) => {
  res.json(demoState);
});

app.post("/api/demo/reset", async (_req, res) => {
  demoState.prod_a_id = null;
  demoState.erp_id = null;
  demoState.version_id = null;
  demoState.step = 0;
  resetAllData();
  seedDemoUsers();
  // Reset downstream products
  try { await fetch("http://product-a:3002/api/reset", { method: "POST" }); } catch {}
  try { await fetch("http://product-b:3003/api/reset", { method: "POST" }); } catch {}
  console.log("[DEMO] State reset (including mapper data + products)");
  res.json({ ok: true, message: "Demo state reset" });
});

// Step 1: ERP publishes reference data + Platform configures economic model + dimension catalog
app.post("/api/demo/step/1", async (_req, res) => {
  try {
    const r = await fetch(`${ERP_URL}/api/publish-accounts`, { method: "POST" });
    const data = await r.json() as any;

    // Configure flex-dimension model (structural setup, done once at onboarding)
    configureDimModel("prod_a", "dim1", "Activity");
    configureDimModel("prod_a", "dim2", "Cost Center");
    configureDimModel("prod_a", "dim3", "Counterpart");
    configureDimModel("prod_b", "dim1", "Activity");
    configureDimModel("prod_b", "dim2", "Cost Center");
    configureDimModel("prod_b", "dim3", "Counterpart");

    // ERP (named fields) → Product A (flex-dim slots)
    configureDimRouting("erp", "activity", "prod_a", "dim1");
    configureDimRouting("erp", "cost_bearer", "prod_a", "dim2");
    configureDimRouting("erp", "counterpart", "prod_a", "dim3");

    // ERP (named fields) → Product B (flex-dim slots)
    configureDimRouting("erp", "activity", "prod_b", "dim1");
    configureDimRouting("erp", "cost_bearer", "prod_b", "dim2");
    configureDimRouting("erp", "counterpart", "prod_b", "dim3");

    // Product A (flex-dim slots) → Product B (passthrough, same slot names)
    configureDimRouting("prod_a", "dim1", "prod_b", "dim1");
    configureDimRouting("prod_a", "dim2", "prod_b", "dim2");
    configureDimRouting("prod_a", "dim3", "prod_b", "dim3");

    // ── Register shared dimensions (customer activation from Economy Domain) ──
    registerSharedDimension("account", "Account", "erp", "shared", "account");
    registerSharedDimension("org_unit", "Org Unit", "erp", "shared", "hierarchy");
    registerSharedDimension("project", "Project", "erp", "shared", "flat");

    // Register which products participate in each dimension
    registerParticipant("account", "erp", "producer");
    registerParticipant("account", "prod_a", "both");
    registerParticipant("account", "prod_b", "consumer");
    registerParticipant("org_unit", "erp", "producer");
    registerParticipant("org_unit", "prod_a", "both");
    registerParticipant("org_unit", "prod_b", "consumer");
    registerParticipant("project", "erp", "producer");
    registerParticipant("project", "prod_a", "producer");
    registerParticipant("project", "prod_b", "consumer");

    // System config for deep links (inbox → product UI) and system registry
    setSystemConfig("erp", "system_type", "erp");
    setSystemConfig("prod_a", "task_base_url", "http://localhost:3002");
    setSystemConfig("prod_a", "system_type", "budgeting");
    setSystemConfig("prod_b", "task_base_url", "http://localhost:3003");
    setSystemConfig("prod_b", "system_type", "analytics");

    // ── Economy Domain: stage entities + relations from ERP data ──
    // Economy domain is now single source of truth for dimension codes/hierarchy
    const accounts = data.event?.accounts || data.accounts || [];
    const orgUnits = data.event?.org_units || data.org_units || [];
    let econEntities = 0, econRelations = 0;
    for (const acc of accounts) {
      upsertEconEntity({ source_system: "erp", dimension: "account", code: acc.code, name: acc.name, type: acc.type || "leaf" });
      econEntities++;
      if (acc.parent) { upsertEconRelation({ source_system: "erp", dimension: "account", child_code: acc.code, parent_code: acc.parent, hierarchy_name: "standard" }); econRelations++; }
    }
    for (const org of orgUnits) {
      upsertEconEntity({ source_system: "erp", dimension: "org_unit", code: org.code, name: org.name, type: org.type || "leaf" });
      econEntities++;
      if (org.parent) { upsertEconRelation({ source_system: "erp", dimension: "org_unit", child_code: org.code, parent_code: org.parent, hierarchy_name: "standard" }); econRelations++; }
    }
    // Extra hierarchy node + entity not in ERP source data
    upsertEconEntity({ source_system: "erp", dimension: "org_unit", code: "DIV-01", name: "Division South", type: "group" });
    upsertEconRelation({ source_system: "erp", dimension: "org_unit", child_code: "OU-100", parent_code: "DIV-01", hierarchy_name: "standard", level: 1 });
    upsertEconRelation({ source_system: "erp", dimension: "org_unit", child_code: "OU-200", parent_code: "DIV-01", hierarchy_name: "standard", level: 1 });

    // Attribute definitions + sample values (via economy domain)
    upsertEconAttributeDef({ dimension: "account", attribute_name: "account_type", attribute_label: "Account Type", source_system: "erp" });
    upsertEconAttributeDef({ dimension: "account", attribute_name: "account_group", attribute_label: "Account Group", source_system: "erp" });
    upsertEconAttributeDef({ dimension: "org_unit", attribute_name: "region", attribute_label: "Region", source_system: "erp" });
    upsertEconAttributeDef({ dimension: "org_unit", attribute_name: "level", attribute_label: "Level", source_system: "erp" });

    upsertEconEntityAttribute({ dimension: "account", code: "4010", attribute_name: "account_type", attribute_value: "expense", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "account", code: "4010", attribute_name: "account_group", attribute_value: "personnel", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "account", code: "4020", attribute_name: "account_type", attribute_value: "expense", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "account", code: "4020", attribute_name: "account_group", attribute_value: "external services", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "account", code: "5010", attribute_name: "account_type", attribute_value: "expense", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "account", code: "5010", attribute_name: "account_group", attribute_value: "travel", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "org_unit", code: "OU-100", attribute_name: "region", attribute_value: "Stockholm", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "org_unit", code: "OU-100", attribute_name: "level", attribute_value: "department", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "org_unit", code: "OU-200", attribute_name: "region", attribute_value: "Stockholm", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "org_unit", code: "OU-200", attribute_name: "level", attribute_value: "department", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "org_unit", code: "DIV-01", attribute_name: "region", attribute_value: "Stockholm", source_system: "erp" });
    upsertEconEntityAttribute({ dimension: "org_unit", code: "DIV-01", attribute_name: "level", attribute_value: "division", source_system: "erp" });

    // ── ERP flex dimensions → platform dim-slots (dim1/dim2/dim3) ──
    const flexDims = [
      { dim: "dim1", codes: [
        { code: "AKT-100", name: "Design" },
        { code: "AKT-200", name: "Construction" },
        { code: "AKT-300", name: "Inspection" },
      ]},
      { dim: "dim2", codes: [
        { code: "KB-500", name: "Internal" },
        { code: "KB-600", name: "External" },
      ]},
      { dim: "dim3", codes: [
        { code: "MP-200", name: "Supplier Alpha" },
        { code: "MP-300", name: "Supplier Beta" },
      ]},
    ];
    for (const fd of flexDims) {
      for (const c of fd.codes) {
        upsertEconEntity({ source_system: "erp", dimension: fd.dim, code: c.code, name: c.name, type: "leaf" });
        econEntities++;
      }
    }

    upsertSyncState("erp", "entities", { last_sync_at: new Date().toISOString(), rows_received: econEntities + 1, rows_validated: econEntities + 1, status: "idle" });
    upsertSyncState("erp", "relations", { last_sync_at: new Date().toISOString(), rows_received: econRelations + 2, rows_validated: econRelations + 2, status: "idle" });
    console.log(`[DEMO] Economy Domain: staged ${econEntities + 1} entities, ${econRelations + 2} relations from ERP`);

    demoState.step = 1;
    console.log("[DEMO] Step 1: Reference data + economic model + dimension catalog");

    // Seed inbox task (platform-generated, visible to all admins)
    addInboxItem({
      id: "platform:task-review-setup",
      source: "platform",
      type: "review",
      category: "review",
      title: "Review initial setup",
      description: "Verify that dimensions, economic model and routing are correctly configured.",
      priority: "normal",
      assigned_to: "user-000", // Platform Admin
      task_path: "/admin.html",
    });

    res.json({ ok: true, step: 1, description: `Economy Domain staged: ${econEntities + 1} entities (incl. flex dims), ${econRelations + 2} relations. Activated 3 shared dimensions + configured routing.`, economy: { entities: econEntities + 1, relations: econRelations + 2 } });
  } catch (err) {
    res.status(500).json({ error: `Could not reach ERP: ${err}` });
  }
});

// Step 2: IdP provisions users via SCIM
app.post("/api/demo/step/2", async (_req, res) => {
  try {
    // Simulate IdP pushing 3 users via SCIM 2.0 (like Microsoft Entra ID would)
    const scimUsers = [
      { externalId: "entra-a1b2c3", userName: "lisa.berg", displayName: "Lisa Berg", emails: [{ value: "lisa.berg@acme.se", primary: true }], groups: [{ display: "role:controller" }, { display: "org:OU-100" }, { display: "product:prod_a" }, { display: "product:prod_b" }, { display: "controllers" }], active: true },
      { externalId: "entra-d4e5f6", userName: "omar.hassan", displayName: "Omar Hassan", emails: [{ value: "omar.hassan@acme.se", primary: true }], groups: [{ display: "role:analyst" }, { display: "org:OU-200" }, { display: "product:prod_b" }, { display: "analysts" }], active: true },
      { externalId: "entra-g7h8i9", userName: "maria.silva", displayName: "Maria Silva", emails: [{ value: "maria.silva@acme.se", primary: true }], groups: [{ display: "role:controller" }, { display: "org:OU-300" }, { display: "product:prod_a" }, { display: "controllers" }], active: true },
    ];
    const provisionedUsers: any[] = [];
    for (const scimUser of scimUsers) {
      const groupNames = scimUser.groups.map(g => g.display);
      const claims = parseGroupClaims(groupNames);
      const user = upsertUser({
        user_id: "user-" + String(Date.now()).slice(-6) + String(Math.random()).slice(-2),
        external_id: scimUser.externalId,
        username: scimUser.userName,
        name: scimUser.displayName,
        email: scimUser.emails[0]?.value || null,
        role: claims.role,
        org_unit: claims.org_unit,
        products: claims.products.length ? claims.products : undefined,
        primary_product: claims.primary_product,
        groups: claims.plainGroups,
        status: scimUser.active ? "active" : "suspended",
        source: "scim",
        password_hash: "demo",
      });
      provisionedUsers.push({ user_id: user.user_id, name: user.name, source: user.source, external_id: user.external_id });
    }

    demoState.step = 2;
    console.log(`[DEMO] Step 2: SCIM provisioned ${provisionedUsers.length} users from IdP`);
    res.json({ ok: true, step: 2, description: `IdP provisioned ${provisionedUsers.length} users via SCIM (Lisa Berg, Omar Hassan, Maria Silva) — source: scim, external_id from Entra`, users: provisionedUsers });
  } catch (err) {
    res.status(500).json({ error: `SCIM provisioning failed: ${err}` });
  }
});

// Step 3: ERP creates a project
app.post("/api/demo/step/3", async (_req, res) => {
  try {
    const r = await fetch(`${ERP_URL}/api/create-project`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "New Office Building" }),
    });
    const data = await r.json() as any;
    demoState.erp_id = data.event?.erp_id;
    demoState.step = 3;
    console.log(`[DEMO] Step 3: ERP project ${demoState.erp_id}`);
    res.json({ ok: true, step: 3, description: `ERP project created: ${demoState.erp_id}`, data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach ERP: ${err}` });
  }
});

// Step 4: ERP publishes actuals (with activity dimension)
app.post("/api/demo/step/4", async (_req, res) => {
  if (!demoState.erp_id) {
    res.status(400).json({ error: "Run step 3 first — ERP project missing" });
    return;
  }
  try {
    // Call ERP without entries → uses full generated GL data (all accounts, org units, 12 months)
    const r = await fetch(`${ERP_URL}/api/publish-gl`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        erp_id: demoState.erp_id,
      }),
    });
    const data = await r.json() as any;
    const count = data?.event?.entries?.length || 0;

    // ── Economy Domain: stage GL facts ──
    const glEntries = data?.event?.entries || [];
    if (glEntries.length > 0) {
      const facts = glEntries.map((e: any) => ({
        source_system: "erp",
        source_batch_id: `erp-gl-${demoState.erp_id}`,
        project_id: demoState.erp_id,
        account: e.account,
        org_unit: e.org_unit,
        period: e.period,
        amount: e.amount,
        currency: e.currency || "SEK",
        transaction_date: e.transaction_date,
        dim1: e.activity || null,
        dim2: e.cost_bearer || null,
        dim3: e.counterpart || null,
      }));
      const insertResult = insertEconFacts(facts);
      const valResult = validateEconFacts(insertResult.batch_id);
      upsertSyncState("erp", "facts", {
        last_sync_at: new Date().toISOString(),
        rows_received: insertResult.received,
        rows_validated: valResult.validated,
        rows_rejected: valResult.rejected,
        status: "idle",
      });
      console.log(`[DEMO] Economy Domain: staged ${insertResult.received} facts, validated ${valResult.validated}, rejected ${valResult.rejected}`);
    }

    demoState.step = 4;
    console.log(`[DEMO] Step 4: GL published for ${demoState.erp_id} (${count} entries)`);
    res.json({ ok: true, step: 4, description: `Published ${count} GL entries from ERP for project ${demoState.erp_id}. Economy Domain: ${count} facts staged + validated.`, data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach ERP: ${err}` });
  }
});

// ── Fetch actuals — callable by products to trigger ERP GL publish ──
app.post("/api/fetch-actuals", async (_req, res) => {
  if (demoState.erp_id) {
    // Demo flow: fetch live from ERP
    try {
      const r = await fetch(`${ERP_URL}/api/publish-gl`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ erp_id: demoState.erp_id }),
      });
      const data = await r.json() as any;
      const count = data?.event?.entries?.length || 0;
      console.log(`[PLATFORM] Fetch actuals: ${count} GL entries from ERP (${demoState.erp_id})`);
      res.json({ ok: true, entries: count, erp_id: demoState.erp_id });
    } catch (err) {
      res.status(500).json({ error: `Could not reach ERP: ${err}` });
    }
  } else {
    // Non-demo: publish validated facts from Economy Domain directly to Kafka
    const facts = getEconFactsForPublish();
    if (facts.length === 0) {
      res.json({ ok: true, entries: 0, message: "All facts already published — data should be visible in products" });
      return;
    }
    try {
      const producer = kafka.producer();
      await producer.connect();
      // Group by project
      const byProject = new Map<string, any[]>();
      for (const f of facts) {
        const key = f.project_id || "_no_project";
        if (!byProject.has(key)) byProject.set(key, []);
        byProject.get(key)!.push(f);
      }
      for (const [projectId, entries] of byProject) {
        await producer.send({
          topic: "platform.projects.out",
          messages: [{ key: projectId, value: JSON.stringify({ source_system: "erp", source_key: projectId, name: projectId }) }],
        });
        const glEntries = entries.map(e => ({
          account: e.account, org_unit: e.org_unit, amount: e.amount,
          currency: e.currency || "SEK", period: e.period,
          transaction_date: e.transaction_date || null,
          activity: e.dim1, cost_bearer: e.dim2, counterpart: e.dim3,
        }));
        // Apply dim routing rules — without rules, dims are NULL
        const dimValuesPerEntry = entries.map(e => {
          const sourceData: Record<string, unknown> = {
            activity: e.dim1, cost_center: e.dim2, counterpart: e.dim3,
            dim1: e.dim1, dim2: e.dim2, dim3: e.dim3,
          };
          return applyDimRouting(entries[0].source_system || "erp", "prod_b", sourceData);
        });
        await producer.send({
          topic: "platform.gl.out",
          messages: [{ key: projectId, value: JSON.stringify({
            source_system: entries[0].source_system || "erp",
            source_key: projectId,
            dim_values_per_entry: dimValuesPerEntry,
            original: { erp_id: projectId, entries: glEntries },
          }) }],
        });
      }
      await producer.disconnect();
      const count = publishEconFacts();
      res.json({ ok: true, entries: count, source: "economy-domain" });
    } catch (err) {
      res.status(500).json({ error: `Publish failed: ${err}` });
    }
  }
});

// Step 5: Product A creates a budget project
app.post("/api/demo/step/5", async (_req, res) => {
  try {
    const r = await fetch(`${PRODUCT_A_URL}/api/projects`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "New Office Building — planning" }),
    });
    const data = await r.json() as any;
    demoState.prod_a_id = data.event?.prod_a_id;
    demoState.step = 5;
    console.log(`[DEMO] Step 5: Budget project ${demoState.prod_a_id}`);
    res.json({ ok: true, step: 5, description: `Budget project created: ${demoState.prod_a_id}`, data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach Product A: ${err}` });
  }
});

// Step 6: Product A enters budget
app.post("/api/demo/step/6", async (_req, res) => {
  if (!demoState.prod_a_id) {
    res.status(400).json({ error: "Run step 5 first — budget project missing" });
    return;
  }
  try {
    const r = await fetch(`${PRODUCT_A_URL}/api/budget`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        prod_a_id: demoState.prod_a_id,
        lines: [
          { account: "4010", org_unit: "OU-100", amount: 500000, currency: "SEK", period: "2025-01", dim1: "AKT-100", dim2: "KB-500", dim3: "MP-200" },
          { account: "4020", org_unit: "OU-100", amount: 200000, currency: "SEK", period: "2025-01", dim1: "AKT-200", dim2: "KB-600", dim3: "MP-300" },
        ],
      }),
    });
    const data = await r.json() as any;
    demoState.version_id = data.version_id || null;
    demoState.step = 6;
    console.log(`[DEMO] Step 6: Budget saved as draft for ${demoState.prod_a_id}`);
    res.json({ ok: true, step: 6, description: `Budget saved as draft: 500k (4010) + 200k (4020) period 2025-01 → ${demoState.prod_a_id}`, data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach Product A: ${err}` });
  }
});

// Step 7: Platform: Configure dimension mapping (planning-dims)
app.post("/api/demo/step/7", async (_req, res) => {
  if (!demoState.prod_a_id || !demoState.version_id) {
    res.status(400).json({ error: "Run step 6 first — budget version missing" });
    return;
  }
  try {
    // Planning dimension mapping (per budget version) — uses source identity
    const mapping = getOrCreateDimensionMapping(
      "prod_a", demoState.prod_a_id, "Budget 2025", "2025", demoState.version_id
    );

    demoState.step = 7;
    const dimModel = getDimModel("prod_b");
    console.log(`[DEMO] Step 7: Dimension mapping configured: ${JSON.stringify(mapping)}`);
    res.json({
      ok: true, step: 7,
      description: `Dimension mapping: "Budget 2025" → ${mapping.planning_type} ${mapping.planning_year} v${mapping.planning_version}. Flex-dims configured in step 1 (dim1=${dimModel.dim1}, dim2=${dimModel.dim2}, dim3=${dimModel.dim3})`,
      mapping,
      dim_model: dimModel,
      source_system: "prod_a",
      source_key: demoState.prod_a_id,
    });
  } catch (err) {
    res.status(500).json({ error: `Could not configure mapping: ${err}` });
  }
});

// Step 8: Product A submits budget (publishes to Kafka, Platform enriches with planning dimensions)
app.post("/api/demo/step/8", async (_req, res) => {
  if (!demoState.version_id) {
    res.status(400).json({ error: "Run step 6 first — no budget version to submit" });
    return;
  }
  try {
    const r = await fetch(`${PRODUCT_A_URL}/api/budget-versions/${demoState.version_id}/submit`, {
      method: "POST",
    });
    const data = await r.json() as any;
    demoState.step = 8;
    console.log(`[DEMO] Step 8: Budget submitted ${demoState.version_id}`);
    res.json({ ok: true, step: 8, description: `Budget submitted! Platform uses the configured dimension mapping and routes to Product B`, data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach Product A: ${err}` });
  }
});

// Step 9: Economy Domain: Link projects (identity resolution — business decision, not infra)
app.post("/api/demo/step/9", async (_req, res) => {
  if (!demoState.prod_a_id || !demoState.erp_id) {
    res.status(400).json({ error: "Run steps 3 and 5 first — both projects are needed" });
    return;
  }
  try {
    const entities = [
      { source_system: "erp", source_key: demoState.erp_id, name: "New Office Building" },
      { source_system: "prod_a", source_key: demoState.prod_a_id, name: "New Office Building — planning" },
    ];
    // Record same_as relation in economy domain
    upsertEconRelation({
      source_system: "economy_domain",
      relation_type: "same_as",
      dimension: "project",
      child_code: `prod_a:${demoState.prod_a_id}`,
      parent_code: `erp:${demoState.erp_id}`,
      hierarchy_name: "identity",
    });
    // Publish EntityLinked event
    const event = await publishEntityLinked("project", entities);
    demoState.step = 9;
    console.log(`[DEMO] Step 9: Economy Domain linked erp:${demoState.erp_id} ↔ prod_a:${demoState.prod_a_id}`);
    res.json({ ok: true, step: 9, description: `Economy Domain: erp:${demoState.erp_id} ↔ prod_a:${demoState.prod_a_id} are the same project`, data: { event } });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    res.status(400).json({ error: message });
  }
});

// Step 10: Fetch analytics from Product B
app.post("/api/demo/step/10", async (_req, res) => {
  try {
    const r = await fetch(`${PRODUCT_B_URL}/api/analytics`);
    const analytics = await r.json() as any[];
    demoState.step = 10;
    console.log(`[DEMO] Step 10: Analytics shows ${analytics.length} rows`);
    res.json({ ok: true, step: 10, description: `Product B shows ${analytics.length} analytics row(s) — budget with planning dimensions + actuals with flex dimensions (dim1-dim3)`, analytics });
  } catch (err) {
    res.status(500).json({ error: `Could not reach Product B: ${err}` });
  }
});

// Step 11: Assign users to budget version and open it (creates inbox tasks)
app.post("/api/demo/step/11", async (_req, res) => {
  if (!demoState.version_id) {
    res.status(400).json({ error: "Run step 6 first — budget version missing" });
    return;
  }
  try {
    // Set org_root on the existing budget version
    await fetch(`${PRODUCT_A_URL}/api/budget-versions/${demoState.version_id}/org-root`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ org_root: "ACME" }),
    });

    // Assign anna → DEPT-A (group — she handles Sales + Marketing), calle → OU-300 (IT)
    const assignRes = await fetch(`${PRODUCT_A_URL}/api/budget-versions/${demoState.version_id}/assignments`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        assignments: [
          { org_unit: "DEPT-A", user_id: "user-001", user_name: "Anna Svensson" },
          { org_unit: "OU-300", user_id: "user-003", user_name: "Calle Björk" },
        ],
      }),
    });
    await assignRes.json();

    // Open the version → creates tasks in inbox
    const openRes = await fetch(`${PRODUCT_A_URL}/api/budget-versions/${demoState.version_id}/open`, { method: "PUT" });
    const openData = await openRes.json() as any;

    demoState.step = 11;
    console.log(`[DEMO] Step 11: Budget version opened, ${openData.tasks_created} tasks created`);
    res.json({
      ok: true, step: 11,
      description: `Budget version "${demoState.version_id}" opened — ${openData.tasks_created} tasks created. Anna → DEPT-A (group), Calle → OU-300.`,
      version_id: demoState.version_id,
      tasks_created: openData.tasks_created,
    });
  } catch (err) {
    res.status(500).json({ error: `Process management demo failed: ${err}` });
  }
});

// ── Lab Scenario: Inject invalid GL rows to demonstrate rejected facts ──
app.post("/api/demo/inject-bad-facts", (_req, res) => {
  const badFacts = [
    { source_system: "erp", source_batch_id: "lab-bad-facts", account: "9999", org_unit: "OU-100", period: "2025-03", amount: 42000, currency: "SEK" },
    { source_system: "erp", source_batch_id: "lab-bad-facts", account: "4010", org_unit: "OU-FAKE", period: "2025-04", amount: 88000, currency: "SEK" },
    { source_system: "erp", source_batch_id: "lab-bad-facts", account: "0000", org_unit: "OU-GHOST", period: "2025-05", amount: 13500, currency: "SEK" },
    { source_system: "erp", source_batch_id: "lab-bad-facts", account: "XXXX", org_unit: "OU-300", period: "2025-06", amount: -55000, currency: "SEK" },
    { source_system: "erp", source_batch_id: "lab-bad-facts", account: "3010", org_unit: "OU-999", period: "2025-07", amount: -200000, currency: "SEK" },
  ];
  const insertResult = insertEconFacts(badFacts);
  const valResult = validateEconFacts(insertResult.batch_id);
  upsertSyncState("erp", "facts", {
    last_sync_at: new Date().toISOString(),
    rows_received: insertResult.received,
    rows_validated: valResult.validated,
    rows_rejected: valResult.rejected,
    status: "idle",
  });
  console.log(`[LAB] Injected ${insertResult.received} bad facts → ${valResult.validated} validated, ${valResult.rejected} rejected`);
  res.json({
    ok: true,
    description: `Injected ${insertResult.received} rows: ${valResult.rejected} rejected, ${valResult.validated} validated. Check "Rejected Facts" in Data Operations.`,
    validated: valResult.validated,
    rejected: valResult.rejected,
    errors: valResult.errors,
  });
});

// ── Golden Path — Full lifecycle demo ──
app.post("/api/demo/golden-path", async (_req, res) => {
  const steps: Array<{ step: string; result: string; ok: boolean }> = [];
  try {
    // Step 1: Full sync (entities + facts)
    await runEconSync("erp", "all");
    const s1 = getEconFactsSummary();
    steps.push({ step: "Sync entities + facts", result: `${s1.total} facts, ${s1.validated} validated`, ok: true });

    // Step 2: Re-sync same data to trigger change detection ("unchanged, skipping publish")
    await runEconSync("erp", "entities");
    steps.push({ step: "Re-sync (change detection)", result: "Dimension publish skipped — content hash unchanged", ok: true });

    // Step 3: Inject invalid rows
    const badFacts = [
      { source_system: "erp", source_batch_id: "golden-path-bad", account: "9999", org_unit: "OU-100", period: "2025-08", amount: 50000, currency: "SEK" },
      { source_system: "erp", source_batch_id: "golden-path-bad", account: "4010", org_unit: "OU-FAKE", period: "2025-09", amount: 75000, currency: "SEK" },
      { source_system: "erp", source_batch_id: "golden-path-bad", account: "XXXX", org_unit: "OU-300", period: "2025-10", amount: -30000, currency: "SEK" },
    ];
    const ins = insertEconFacts(badFacts);
    const val = validateEconFacts(ins.batch_id);
    steps.push({ step: "Inject invalid GL rows", result: `${ins.received} inserted → ${val.rejected} rejected, ${val.validated} validated`, ok: true });

    // Step 4: Re-validate (will still fail because reference data doesn't exist)
    const reval1 = revalidateEconFacts();
    steps.push({ step: "Re-validate (before fix)", result: `${reval1.reset} reset → ${reval1.rejected} still rejected (expected)`, ok: true });

    // Step 5: Fix reference data (add the missing entities) then re-validate
    upsertEconEntity({ source_system: "erp", dimension: "account", code: "9999", name: "Suspense Account (recovered)", type: "leaf" });
    upsertEconEntity({ source_system: "erp", dimension: "org_unit", code: "OU-FAKE", name: "Fake Unit (recovered)", type: "leaf" });
    upsertEconEntity({ source_system: "erp", dimension: "org_unit", code: "OU-300", name: "Branch Office (recovered)", type: "leaf" });
    // Note: "XXXX" account still doesn't exist — shows partial recovery
    const reval2 = revalidateEconFacts();
    steps.push({ step: "Fix reference data + re-validate", result: `${reval2.reset} reset → ${reval2.validated} recovered, ${reval2.rejected} still rejected`, ok: true });

    // Step 6: Publish all validated facts
    const pub = await executeFactPublish();
    steps.push({ step: "Final publish", result: `${pub.published} facts delivered to downstream products`, ok: true });

    insertAuditEvent("out", "demo.golden-path", "GoldenPath", undefined, undefined, `Golden Path complete: ${steps.length} steps, ${pub.published} facts published`);
    res.json({ ok: true, steps });
  } catch (err: any) {
    steps.push({ step: "ERROR", result: err.message, ok: false });
    res.json({ ok: false, steps, error: err.message });
  }
});

// Fallback: serve login page for root
app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "login.html"));
});

// ── Help Articles API ──

app.get("/api/help", (req: any, res) => {
  const { q, products } = req.query;
  if (q) {
    const productList = products ? (products as string).split(",") : undefined;
    res.json(searchHelpArticles(q as string, productList));
  } else {
    res.json(getAllHelpArticles());
  }
});

app.get("/api/help/user", (req: any, res) => {
  // Return articles filtered by user's product access
  const products = req.query.products ? (req.query.products as string).split(",") : [];
  res.json(getHelpArticlesForUser(products));
});

app.get("/api/help/slug/:slug", (req, res) => {
  const article = getHelpArticleBySlug(req.params.slug);
  if (!article) { res.status(404).json({ error: "Not found" }); return; }
  res.json(article);
});

app.get("/api/help/:id", (req, res) => {
  const article = getHelpArticle(Number(req.params.id));
  if (!article) { res.status(404).json({ error: "Not found" }); return; }
  res.json(article);
});

app.post("/api/help", (req, res) => {
  const { slug, title, product, category, body_md, keywords, sort_order } = req.body;
  if (!slug || !title) { res.status(400).json({ error: "slug and title required" }); return; }
  try {
    const article = createHelpArticle({ slug, title, product, category, body_md, keywords, sort_order });
    res.status(201).json(article);
  } catch (err: any) {
    res.status(409).json({ error: err.message });
  }
});

app.put("/api/help/:id", (req, res) => {
  const article = updateHelpArticle(Number(req.params.id), req.body);
  if (!article) { res.status(404).json({ error: "Not found" }); return; }
  res.json(article);
});

app.delete("/api/help/:id", (req, res) => {
  const ok = deleteHelpArticle(Number(req.params.id));
  res.json({ ok });
});

// ── Start ──

async function start() {
  await startRouter(kafka);

  app.listen(PORT, () => {
    console.log(`[PLATFORM] Running on http://localhost:${PORT}`);
    console.log("[PLATFORM] Endpoints:");
    console.log("  POST /api/login                    — Log in (anna/demo or erik/demo)");
    console.log("  POST /api/logout                   — Log out");
    console.log("  GET  /api/me                       — Who is logged in?");
    console.log("  POST /api/economy/link-entities     — Economy domain: identity resolution");
  });
}

start().catch((err) => {
  console.error("[PLATFORM] Failed to start:", err);
  process.exit(1);
});
