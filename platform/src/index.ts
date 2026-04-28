import express from "express";
import cors from "cors";
import cookieParser from "cookie-parser";
import jwt from "jsonwebtoken";
import { Kafka } from "kafkajs";
import path from "path";

import { linkProjects, getAllProjects, getMappings, getOrCreateDimensionMapping, getAllDimensionMappings, updateDimensionMapping, lookupCanonical, configureDimModel, getDimModel, getAllDimModels, configureDimRouting, getAllDimRouting, registerSharedDimension, getAllSharedDimensions, upsertDimensionCode, getDimensionCodes, registerParticipant, getParticipants, upsertCodeMapping, getCodeMappings, registerConnector, registerConnectorDimension, getAllConnectors, getConnectorDimensions, updateConnectorField, registerDimensionAttribute, getDimensionAttributes, setCodeAttribute, getCodeAttributes, getAllCodeAttributes, setHierarchy, getHierarchy, resetAllData, getInboxItems, addInboxItem, updateInboxItem, getConnectorTaskBaseUrl, deleteDimModel, deleteDimRouting, deleteConnector, deleteParticipant } from "./mapper";
import { startRouter, publishLink, getEventLog } from "./router";

// ── Config ──

const PORT = 3000;
const JWT_SECRET = "platform-poc-secret-not-for-production";

const DEMO_USERS = [
  { username: "anna", password: "demo", user_id: "user-001", name: "Anna Svensson", role: "controller", org_unit: "OU-100", products: ["prod_a", "prod_b"], primary_product: "prod_a" },
  { username: "erik", password: "demo", user_id: "user-002", name: "Erik Lindgren", role: "analyst", org_unit: "OU-200", products: ["prod_b"], primary_product: "prod_b" },
  { username: "calle", password: "demo", user_id: "user-003", name: "Calle Björk", role: "controller", org_unit: "OU-100", products: ["prod_a"], primary_product: "prod_a" },
  { username: "admin", password: "demo", user_id: "user-000", name: "Admin User", role: "admin", org_unit: "ACME", products: ["platform", "prod_a", "prod_b"], primary_product: "platform" },
];

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
app.use(express.static(path.join(__dirname, "..", "public")));

// ── Auth endpoints ──

app.post("/api/login", (req, res) => {
  const { username, password } = req.body;
  const user = DEMO_USERS.find((u) => u.username === username && u.password === password);
  if (!user) {
    res.status(401).json({ error: "Invalid credentials" });
    return;
  }
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

app.get("/api/me", (req, res) => {
  const token = req.cookies?.platform_token;
  if (!token) {
    res.status(401).json({ error: "Not logged in" });
    return;
  }
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    res.json(payload);
  } catch {
    res.status(401).json({ error: "Invalid token" });
  }
});

// Users: expose demo users for assignment UIs (filter by product if ?product=xxx)
app.get("/api/users", (_req, res) => {
  const product = _req.query.product as string | undefined;
  let users = DEMO_USERS.map(u => ({
    user_id: u.user_id, name: u.name, role: u.role, org_unit: u.org_unit, products: u.products,
  }));
  if (product) {
    users = users.filter(u => u.products.includes(product));
  }
  res.json(users);
});

// Navigation: dynamic product list based on connected systems + user entitlements
app.get("/api/navigation", (req, res) => {
  const token = req.cookies?.platform_token;
  if (!token) { res.status(401).json({ error: "Not logged in" }); return; }

  let user: any;
  try { user = jwt.verify(token, JWT_SECRET); } catch { res.status(401).json({ error: "Invalid token" }); return; }

  const allowedProducts = user.products || [];
  const connectors = getAllConnectors();

  // Platform Admin is always available for users who have "platform" in their products
  const items: Array<{ key: string; label: string; url: string }> = [];
  if (allowedProducts.includes("platform")) {
    items.push({ key: "platform", label: "Platform Admin", url: "/admin.html" });
  }

  // Internal connected products — use task_base_url for user-facing navigation
  const INTERNAL_TYPES = ["budgeting", "analytics", "planning"];
  for (const c of connectors) {
    if (!INTERNAL_TYPES.includes(c.system_type)) continue;
    if (!allowedProducts.includes(c.system_name)) continue;
    if (!c.task_base_url) continue;
    items.push({ key: c.system_name, label: c.display_name, url: c.task_base_url });
  }

  res.json(items);
});

// ── Platform API ──

app.get("/api/projects", (_req, res) => {
  const projects = getAllProjects();
  res.json(projects);
});

app.get("/api/projects/:canonicalId/mappings", (req, res) => {
  const mappings = getMappings(req.params.canonicalId);
  res.json(mappings);
});

app.post("/api/link", async (req, res) => {
  const { source_id, target_id } = req.body;
  if (!source_id || !target_id) {
    res.status(400).json({ error: "source_id and target_id required" });
    return;
  }
  try {
    const result = linkProjects(source_id, target_id);
    const event = await publishLink(result.canonical_id, result.linked);
    console.log(`[PLATFORM] Linked: ${source_id} ↔ ${target_id} → ${result.canonical_id}`);
    res.json({ ok: true, ...result, event });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : "Unknown error";
    res.status(400).json({ error: message });
  }
});

// Healthcheck
app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "platform" });
});

// Event log
app.get("/api/events", (req, res) => {
  const limit = Math.min(parseInt(req.query?.limit as string) || 100, 500);
  res.json(getEventLog(limit));
});

// ── Dimension mappings API ──

app.get("/api/dimension-mappings", (_req, res) => {
  res.json(getAllDimensionMappings());
});

app.post("/api/dimension-mappings/configure", (req, res) => {
  const { canonical_id, source_version_id, version_name, year } = req.body;
  if (!canonical_id || !version_name || !year) {
    res.status(400).json({ error: "canonical_id, version_name and year required" });
    return;
  }
  const mapping = getOrCreateDimensionMapping(canonical_id, version_name, year, source_version_id);
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

app.delete("/api/connectors/:system", (req, res) => {
  deleteConnector(req.params.system);
  res.json({ ok: true });
});

app.delete("/api/shared-dimensions/:name/participants/:product", (req, res) => {
  deleteParticipant(req.params.name, req.params.product);
  res.json({ ok: true });
});

// ── Shared Dimension Catalog API ──

// ── Connector Registry API ──

// Connector catalog — available adapter types that can be used to connect systems
const CONNECTOR_CATALOG = [
  { id: "hypergene-erp", label: "ERP Adapter", description: "For systems handling accounting, general ledger and reference data (chart of accounts, org units)", protocol: "rest", capabilities_path: "/api/capabilities" },
  { id: "hypergene-budget", label: "Budget Adapter", description: "For budget tools that create and submit budgets", protocol: "rest", capabilities_path: "/api/capabilities" },
  { id: "hypergene-analytics", label: "Analytics Adapter", description: "For analytics and reporting systems that consume data", protocol: "rest", capabilities_path: "/api/capabilities" },
];

app.get("/api/connector-catalog", (_req, res) => {
  res.json(CONNECTOR_CATALOG);
});

app.get("/api/connectors", (_req, res) => {
  res.json(getAllConnectors());
});

app.post("/api/connectors/register-capabilities", (req, res) => {
  const { system_name, system_type, display_name, api_base_url, dimensions } = req.body;
  if (!system_name || !system_type || !display_name) {
    res.status(400).json({ error: "system_name, system_type and display_name required" });
    return;
  }
  registerConnector(system_name, system_type, display_name, api_base_url);
  if (Array.isArray(dimensions)) {
    for (const dim of dimensions) {
      if (dim.field_name && dim.field_label) {
        registerConnectorDimension(system_name, dim.field_name, dim.field_label, dim.data_type || "string");
      }
    }
  }
  res.json({ ok: true, system_name, dimensions_registered: Array.isArray(dimensions) ? dimensions.length : 0 });
});

app.patch("/api/connectors/:system", (req, res) => {
  const { task_base_url } = req.body;
  if (task_base_url === undefined) { res.status(400).json({ error: "task_base_url required" }); return; }
  updateConnectorField(req.params.system, "task_base_url", task_base_url || null);
  res.json({ ok: true, system_name: req.params.system });
});

app.get("/api/connectors/:system/dimensions", (req, res) => {
  res.json(getConnectorDimensions(req.params.system));
});

// Preview a system's capabilities without registering anything
app.post("/api/connectors/preview", async (req, res) => {
  const { url, connector_type } = req.body;
  if (!url) { res.status(400).json({ error: "url required" }); return; }

  const adapter = connector_type ? CONNECTOR_CATALOG.find(c => c.id === connector_type) : null;
  const capPath = adapter?.capabilities_path || "/api/capabilities";

  try {
    const capUrl = url.replace(/\/+$/, "") + capPath;
    const r = await fetch(capUrl, { signal: AbortSignal.timeout(5000) });
    if (!r.ok) { res.status(502).json({ error: `System svarade med ${r.status}` }); return; }
    const caps = await r.json() as any;

    if (!caps.system_name || !caps.system_type || !caps.display_name) {
      res.status(502).json({ error: "Systemet saknar system_name, system_type eller display_name" }); return;
    }

    // Classify fields without registering
    const sharedDims = getAllSharedDimensions();
    const fields: any[] = [];
    if (Array.isArray(caps.data_fields)) {
      for (const field of caps.data_fields) {
        if (!field.field_name) continue;
        if (field.shared_dimension) {
          fields.push({ ...field, category: "shared", selected: true });
        } else {
          fields.push({ ...field, category: "routing", selected: true });
        }
      }
    }

    res.json({
      ok: true,
      system_name: caps.system_name,
      system_type: caps.system_type,
      display_name: caps.display_name,
      task_base_url: caps.task_base_url || null,
      fields,
    });
  } catch (e: any) {
    res.status(502).json({ error: `Could not connect: ${e.message || e}` });
  }
});

// Auto-discover a system by calling its /api/capabilities endpoint
app.post("/api/connectors/discover", async (req, res) => {
  const { url, connector_type, selected_fields } = req.body;
  if (!url) { res.status(400).json({ error: "url required" }); return; }

  // Look up adapter from catalog (optional but informative)
  const adapter = connector_type ? CONNECTOR_CATALOG.find(c => c.id === connector_type) : null;
  const capPath = adapter?.capabilities_path || "/api/capabilities";

  try {
    const capUrl = url.replace(/\/+$/, "") + capPath;
    const r = await fetch(capUrl, { signal: AbortSignal.timeout(5000) });
    if (!r.ok) { res.status(502).json({ error: `System svarade med ${r.status}` }); return; }
    const caps = await r.json() as any;

    if (!caps.system_name || !caps.system_type || !caps.display_name) {
      res.status(502).json({ error: "Systemet saknar system_name, system_type eller display_name" }); return;
    }

    // Register connector (store adapter type if provided, including task_base_url for deep linking)
    const taskBaseUrl = caps.task_base_url || null;
    registerConnector(caps.system_name, caps.system_type, caps.display_name, url, taskBaseUrl);

    // Filter fields if user made a selection in preview
    const selectedSet = Array.isArray(selected_fields) ? new Set(selected_fields as string[]) : null;

    // Process data_fields: match against existing shared dimensions, rest become routing fields
    const sharedDims = getAllSharedDimensions();
    const sharedNames = new Set(sharedDims.map((d: any) => d.name));
    const matchedShared: any[] = [];
    const routingFields: any[] = [];

    // Is this an internal Hypergene product? Internal products never own shared dimensions.
    const INTERNAL_TYPES = ['budgeting', 'analytics', 'planning'];
    const isInternal = INTERNAL_TYPES.includes(caps.system_type);

    if (Array.isArray(caps.data_fields)) {
      for (const field of caps.data_fields) {
        if (!field.field_name) continue;
        if (selectedSet && !selectedSet.has(field.field_name)) continue;
        if (field.shared_dimension) {
          // Field explicitly declares which shared dimension it maps to
          const dimLabel = field.label || field.field_label || field.shared_dimension;
          if (!sharedNames.has(field.shared_dimension)) {
            // Create dimension — only external producers become owner
            const owner = (!isInternal && field.role === 'producer') ? caps.system_name : '';
            registerSharedDimension(field.shared_dimension, dimLabel, owner, "shared", field.taxonomy_type || 'shared');
            sharedNames.add(field.shared_dimension);
          } else if (!isInternal && field.role === 'producer') {
            // External producer claims ownership of existing unclaimed dimension
            const existing = sharedDims.find((d: any) => d.name === field.shared_dimension);
            if (existing && (!existing.owner_system || existing.owner_system === 'pending')) {
              registerSharedDimension(field.shared_dimension, existing.label, caps.system_name, existing.taxonomy_type || 'shared', existing.dimension_type || 'shared');
            }
          }
          matchedShared.push(field);
          registerParticipant(field.shared_dimension, caps.system_name, field.role || 'consumer');
        } else {
          // No shared dimension declared — it's a routing field
          routingFields.push(field);
          registerConnectorDimension(caps.system_name, field.field_name, field.field_label || field.field_name, field.data_type || "string");
        }
      }
    }

    // Legacy support: also handle old shared_dimensions + routing_fields arrays
    if (Array.isArray(caps.routing_fields)) {
      for (const rf of caps.routing_fields) {
        if (rf.field_name && rf.field_label) {
          registerConnectorDimension(caps.system_name, rf.field_name, rf.field_label, rf.data_type || "string");
          routingFields.push(rf);
        }
      }
    }
    if (Array.isArray(caps.shared_dimensions)) {
      for (const sd of caps.shared_dimensions) {
        if (sd.name && sd.role) {
          if (!sharedNames.has(sd.name)) {
            const owner = (!isInternal && sd.role === 'producer') ? caps.system_name : '';
            registerSharedDimension(sd.name, sd.label || sd.name, owner, "shared", sd.taxonomy_type || 'shared');
            sharedNames.add(sd.name);
          } else if (!isInternal && sd.role === 'producer') {
            const existing = sharedDims.find((d: any) => d.name === sd.name);
            if (existing && (!existing.owner_system || existing.owner_system === 'pending')) {
              registerSharedDimension(sd.name, existing.label, caps.system_name, existing.taxonomy_type || 'shared', existing.dimension_type || 'shared');
            }
          }
          registerParticipant(sd.name, caps.system_name, sd.role);
          matchedShared.push(sd);
        }
      }
    }

    console.log(`[DISCOVER] Auto-registered ${caps.system_name} from ${url} (adapter: ${connector_type || 'auto'}, shared: ${matchedShared.length}, routing: ${routingFields.length})`);
    res.json({ ok: true, connector_type: connector_type || null, matched_shared: matchedShared, routing_fields: routingFields, ...caps });
  } catch (e: any) {
    res.status(502).json({ error: `Could not connect: ${e.message || e}` });
  }
});

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
      const baseUrl = item.source === "platform" ? `http://localhost:${PORT}` : (getConnectorTaskBaseUrl(item.source) || FALLBACK_URLS[item.source]);
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

// ── Demo Runner ──
// Orchestrates the 8-step demo from Platform by calling ERP/Product A/Product B APIs

const ERP_URL = process.env.ERP_URL || "http://erp-mock:3001";
const PRODUCT_A_URL = process.env.PRODUCT_A_URL || "http://product-a:3002";
const PRODUCT_B_URL = process.env.PRODUCT_B_URL || "http://product-b:3003";

const demoState = {
  prod_a_id: null as string | null,
  erp_id: null as string | null,
  canonical_id: null as string | null,
  version_id: null as string | null,
  step: 0,
};

app.get("/api/demo/state", (_req, res) => {
  res.json(demoState);
});

app.post("/api/demo/reset", async (_req, res) => {
  demoState.prod_a_id = null;
  demoState.erp_id = null;
  demoState.canonical_id = null;
  demoState.version_id = null;
  demoState.step = 0;
  resetAllData();
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

    // Auto-discover all three systems (calls their /api/capabilities endpoints)
    // ERP is discovered first since it's the owner of shared dimensions
    const discoverUrl = `http://localhost:${PORT}/api/connectors/discover`;
    for (const sysUrl of [ERP_URL, PRODUCT_A_URL, PRODUCT_B_URL]) {
      try {
        const dr = await fetch(discoverUrl, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ url: sysUrl }) });
        if (!dr.ok) { const t = await dr.text(); console.error(`[DEMO] Discover ${sysUrl} failed ${dr.status}: ${t}`); }
      } catch (de: any) { console.error(`[DEMO] Discover ${sysUrl} error: ${de.message}`); }
    }

    // Populate canonical code lists from ERP event data
    for (const acc of data.event?.accounts || data.accounts || []) {
      upsertDimensionCode("account", acc.code, acc.name);
    }
    for (const org of data.event?.org_units || data.org_units || []) {
      upsertDimensionCode("org_unit", org.code, org.name);
    }

    // Dimension attributes — enrich codes with metadata
    registerDimensionAttribute("account", "account_type", "Account Type", "string");
    registerDimensionAttribute("account", "account_group", "Account Group", "string");
    registerDimensionAttribute("org_unit", "region", "Region", "string");
    registerDimensionAttribute("org_unit", "level", "Level", "string");

    // Code attributes — sample data
    setCodeAttribute("account", "4010", "account_type", "expense");
    setCodeAttribute("account", "4010", "account_group", "personnel");
    setCodeAttribute("account", "4020", "account_type", "expense");
    setCodeAttribute("account", "4020", "account_group", "external services");
    setCodeAttribute("account", "5010", "account_type", "expense");
    setCodeAttribute("account", "5010", "account_group", "travel");
    setCodeAttribute("org_unit", "OU-100", "region", "Stockholm");
    setCodeAttribute("org_unit", "OU-100", "level", "department");
    setCodeAttribute("org_unit", "OU-200", "region", "Stockholm");
    setCodeAttribute("org_unit", "OU-200", "level", "department");

    // Hierarchy — org_unit tree
    setHierarchy("org_unit", "OU-100", "DIV-01", 1);
    setHierarchy("org_unit", "OU-200", "DIV-01", 1);
    upsertDimensionCode("org_unit", "DIV-01", "Division South");
    setCodeAttribute("org_unit", "DIV-01", "region", "Stockholm");
    setCodeAttribute("org_unit", "DIV-01", "level", "division");

    demoState.step = 1;
    console.log("[DEMO] Step 1: Reference data + economic model + dimension catalog");

    // Seed cross-system code mappings
    // ERP is source-of-truth — its source_key is the authoritative reference
    upsertCodeMapping("account", "erp", "4010", "4010", "ERP-ACC-001");
    upsertCodeMapping("account", "erp", "4020", "4020", "ERP-ACC-002");
    upsertCodeMapping("account", "erp", "5010", "5010", "ERP-ACC-003");
    upsertCodeMapping("org_unit", "erp", "OU-100", "OU-100", "ERP-ORG-100");
    upsertCodeMapping("org_unit", "erp", "OU-200", "OU-200", "ERP-ORG-200");
    // Product A uses own local codes but references ERP's source key for traceability
    upsertCodeMapping("account", "prod_a", "BUD-4010", "4010", "ERP-ACC-001");
    upsertCodeMapping("account", "prod_a", "BUD-4020", "4020", "ERP-ACC-002");
    upsertCodeMapping("account", "prod_a", "BUD-5010", "5010", "ERP-ACC-003");
    upsertCodeMapping("org_unit", "prod_a", "TEAM-ALFA", "OU-100", "ERP-ORG-100");
    upsertCodeMapping("org_unit", "prod_a", "TEAM-BETA", "OU-200", "ERP-ORG-200");

    // Seed inbox task (platform-generated, visible to all admins)
    addInboxItem({
      id: "platform:task-review-setup",
      source: "platform",
      type: "review",
      category: "review",
      title: "Review initial setup",
      description: "Verify that connectors, dimensions and economic model are correctly configured.",
      priority: "normal",
      assigned_to: "user-000", // Platform Admin
      task_path: "/admin.html",
    });

    res.json({ ok: true, step: 1, description: "ERP published reference data + Platform configured economic model + dimension catalog (3 dimensions, code lists)", data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach ERP: ${err}` });
  }
});

// Step 2: ERP creates a project
app.post("/api/demo/step/2", async (_req, res) => {
  try {
    const r = await fetch(`${ERP_URL}/api/create-project`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "New Office Building" }),
    });
    const data = await r.json() as any;
    demoState.erp_id = data.event?.erp_id;
    demoState.step = 2;
    console.log(`[DEMO] Step 2: ERP project ${demoState.erp_id}`);
    res.json({ ok: true, step: 2, description: `ERP project created: ${demoState.erp_id}`, data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach ERP: ${err}` });
  }
});

// Step 3: ERP publishes actuals (with activity dimension)
app.post("/api/demo/step/3", async (_req, res) => {
  if (!demoState.erp_id) {
    res.status(400).json({ error: "Run step 2 first — ERP project missing" });
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
    demoState.step = 3;
    console.log(`[DEMO] Step 3: GL published for ${demoState.erp_id} (${count} entries)`);
    res.json({ ok: true, step: 3, description: `Published ${count} GL entries from ERP for project ${demoState.erp_id}`, data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach ERP: ${err}` });
  }
});

// ── Fetch actuals — callable by products to trigger ERP GL publish ──
app.post("/api/fetch-actuals", async (_req, res) => {
  if (!demoState.erp_id) {
    res.status(400).json({ error: "No ERP project linked yet — run demo steps 1-2 first" });
    return;
  }
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
});

// Step 4: Product A creates a budget project
app.post("/api/demo/step/4", async (_req, res) => {
  try {
    const r = await fetch(`${PRODUCT_A_URL}/api/projects`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "New Office Building — planning" }),
    });
    const data = await r.json() as any;
    demoState.prod_a_id = data.event?.prod_a_id;
    demoState.step = 4;
    console.log(`[DEMO] Step 4: Budget project ${demoState.prod_a_id}`);
    res.json({ ok: true, step: 4, description: `Budget project created: ${demoState.prod_a_id}`, data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach Product A: ${err}` });
  }
});

// Step 5: Product A enters budget
app.post("/api/demo/step/5", async (_req, res) => {
  if (!demoState.prod_a_id) {
    res.status(400).json({ error: "Run step 4 first — budget project missing" });
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
    demoState.step = 5;
    console.log(`[DEMO] Step 5: Budget saved as draft for ${demoState.prod_a_id}`);
    res.json({ ok: true, step: 5, description: `Budget saved as draft: 500k (4010) + 200k (4020) period 2025-01 → ${demoState.prod_a_id}`, data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach Product A: ${err}` });
  }
});

// Step 6: Platform: Configure dimension mapping (planning-dims)
app.post("/api/demo/step/6", async (_req, res) => {
  if (!demoState.prod_a_id || !demoState.version_id) {
    res.status(400).json({ error: "Run step 5 first — budget version missing" });
    return;
  }
  try {
    const canonicalId = lookupCanonical("prod_a", demoState.prod_a_id);
    if (!canonicalId) {
      res.status(400).json({ error: "Canonical ID missing — run step 4 first" });
      return;
    }
    // Planning dimension mapping (per budget version)
    const mapping = getOrCreateDimensionMapping(
      canonicalId, "Budget 2025", "2025", demoState.version_id
    );

    demoState.step = 6;
    const dimModel = getDimModel("prod_b");
    console.log(`[DEMO] Step 6: Dimension mapping configured: ${JSON.stringify(mapping)}`);
    res.json({
      ok: true, step: 6,
      description: `Dimension mapping: "Budget 2025" → ${mapping.planning_type} ${mapping.planning_year} v${mapping.planning_version}. Flex-dims configured in step 1 (dim1=${dimModel.dim1}, dim2=${dimModel.dim2}, dim3=${dimModel.dim3})`,
      mapping,
      dim_model: dimModel,
      canonical_id: canonicalId,
    });
  } catch (err) {
    res.status(500).json({ error: `Could not configure mapping: ${err}` });
  }
});

// Step 7: Product A submits budget (publishes to Kafka, Platform enriches with planning dimensions)
app.post("/api/demo/step/7", async (_req, res) => {
  if (!demoState.version_id) {
    res.status(400).json({ error: "Run step 5 first — no budget version to submit" });
    return;
  }
  try {
    const r = await fetch(`${PRODUCT_A_URL}/api/budget-versions/${demoState.version_id}/submit`, {
      method: "POST",
    });
    const data = await r.json() as any;
    demoState.step = 7;
    console.log(`[DEMO] Step 7: Budget submitted ${demoState.version_id}`);
    res.json({ ok: true, step: 7, description: `Budget submitted! Platform uses the configured dimension mapping and routes to Product B`, data });
  } catch (err) {
    res.status(500).json({ error: `Could not reach Product A: ${err}` });
  }
});

// Step 8: Link projects
app.post("/api/demo/step/8", async (_req, res) => {
  if (!demoState.prod_a_id || !demoState.erp_id) {
    res.status(400).json({ error: "Run steps 2 and 4 first — both projects are needed" });
    return;
  }
  try {
    const result = linkProjects(demoState.prod_a_id, demoState.erp_id);
    const event = await publishLink(result.canonical_id, result.linked);
    demoState.canonical_id = result.canonical_id;
    demoState.step = 8;
    console.log(`[DEMO] Step 8: Linked ${demoState.prod_a_id} ↔ ${demoState.erp_id} → ${result.canonical_id}`);
    res.json({ ok: true, step: 8, description: `Linked: ${demoState.prod_a_id} ↔ ${demoState.erp_id} → ${result.canonical_id}`, data: { ...result, event } });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    res.status(400).json({ error: message });
  }
});

// Step 9: Fetch analytics from Product B
app.post("/api/demo/step/9", async (_req, res) => {
  try {
    const r = await fetch(`${PRODUCT_B_URL}/api/analytics`);
    const analytics = await r.json() as any[];
    demoState.step = 9;
    console.log(`[DEMO] Step 9: Analytics shows ${analytics.length} rows`);
    res.json({ ok: true, step: 9, description: `Product B shows ${analytics.length} analytics row(s) — budget with planning dimensions + actuals with flex dimensions (dim1-dim3)`, analytics });
  } catch (err) {
    res.status(500).json({ error: `Could not reach Product B: ${err}` });
  }
});

// Step 10: Assign users to budget version and open it (creates inbox tasks)
app.post("/api/demo/step/10", async (_req, res) => {
  if (!demoState.version_id) {
    res.status(400).json({ error: "Run step 5 first — budget version missing" });
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

    demoState.step = 10;
    console.log(`[DEMO] Step 10: Budget version opened, ${openData.tasks_created} tasks created`);
    res.json({
      ok: true, step: 10,
      description: `Budget version "${demoState.version_id}" opened — ${openData.tasks_created} tasks created. Anna → DEPT-A (group), Calle → OU-300.`,
      version_id: demoState.version_id,
      tasks_created: openData.tasks_created,
    });
  } catch (err) {
    res.status(500).json({ error: `Process management demo failed: ${err}` });
  }
});

// Fallback: serve login page for root
app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "login.html"));
});

// ── Start ──

async function start() {
  await startRouter(kafka);

  app.listen(PORT, () => {
    console.log(`[PLATFORM] Running on http://localhost:${PORT}`);
    console.log("[PLATFORM] Endpoints:");
    console.log("  POST /api/login          — Log in (anna/demo or erik/demo)");
    console.log("  POST /api/logout         — Log out");
    console.log("  GET  /api/me             — Who is logged in?");
    console.log("  GET  /api/projects       — All canonical projects + mappings");
    console.log("  POST /api/link           — Link source_id ↔ target_id");
  });
}

start().catch((err) => {
  console.error("[PLATFORM] Failed to start:", err);
  process.exit(1);
});
