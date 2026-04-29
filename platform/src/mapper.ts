import Database from "better-sqlite3";
import path from "path";

const DB_PATH = path.join(__dirname, "..", "data", "platform.db");

// Ensure data directory exists
import fs from "fs";
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

// ── Schema ──

db.exec(`
  CREATE TABLE IF NOT EXISTS canonical_projects (
    canonical_id TEXT PRIMARY KEY,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS id_mappings (
    source_system TEXT NOT NULL,  -- 'erp', 'prod_a', 'prod_b'
    source_id     TEXT NOT NULL,
    canonical_id  TEXT NOT NULL REFERENCES canonical_projects(canonical_id),
    PRIMARY KEY (source_system, source_id)
  );

  CREATE INDEX IF NOT EXISTS idx_mappings_canonical ON id_mappings(canonical_id);

  CREATE TABLE IF NOT EXISTS dimension_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_id       TEXT NOT NULL,
    source_version_id  TEXT,
    planning_year      TEXT NOT NULL,
    planning_type      TEXT NOT NULL,     -- 'Budget', 'F1', 'F2'
    planning_version   INTEGER NOT NULL DEFAULT 1,
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(canonical_id, planning_year, planning_type, planning_version)
  );

  CREATE TABLE IF NOT EXISTS dim_models (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product    TEXT NOT NULL,   -- 'prod_b'
    slot       TEXT NOT NULL,   -- 'dim1', 'dim2', 'dim3'
    label      TEXT NOT NULL,   -- 'Activity', 'Cost Center', 'Counterpart'
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(product, slot)
  );

  CREATE TABLE IF NOT EXISTS dim_routing (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system  TEXT NOT NULL,   -- 'erp'
    source_field   TEXT NOT NULL,   -- 'activity', 'cost_bearer', 'counterpart'
    target_product TEXT NOT NULL,   -- 'prod_b'
    target_slot    TEXT NOT NULL,   -- 'dim1', 'dim2', 'dim3'
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source_system, source_field, target_product)
  );

  -- ── Shared Dimension Catalog ──
  CREATE TABLE IF NOT EXISTS shared_dimensions (
    name           TEXT PRIMARY KEY,  -- 'account', 'org_unit', 'project'
    label          TEXT NOT NULL,     -- 'Account', 'Org Unit'
    owner_system   TEXT NOT NULL,     -- 'erp'
    taxonomy_type  TEXT NOT NULL DEFAULT 'shared',  -- 'shared' or 'mapped'
    dimension_type TEXT NOT NULL DEFAULT 'flat',     -- 'flat', 'hierarchy', 'time', 'account'
    created_at     TEXT NOT NULL DEFAULT (datetime('now'))
  );

  -- dimension_codes: REMOVED — replaced by econ_entities (Economy Domain)
  -- dimension_attributes: REMOVED — replaced by econ_attribute_defs
  -- dimension_code_attributes: REMOVED — replaced by econ_entity_attributes
  -- dimension_hierarchy: REMOVED — replaced by econ_relations

  CREATE TABLE IF NOT EXISTS dimension_participants (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    dimension_name TEXT NOT NULL REFERENCES shared_dimensions(name),
    product        TEXT NOT NULL,     -- 'erp', 'prod_a', 'prod_b'
    role           TEXT NOT NULL,     -- 'producer', 'consumer', 'both'
    uses_canonical INTEGER NOT NULL DEFAULT 1,
    UNIQUE(dimension_name, product)
  );

  CREATE TABLE IF NOT EXISTS dimension_code_mappings (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    dimension_name TEXT NOT NULL,
    product        TEXT NOT NULL,
    local_code     TEXT NOT NULL,
    canonical_code TEXT NOT NULL,
    source_key     TEXT,
    UNIQUE(dimension_name, product, local_code)
  );

  -- ── System Config (key-value per system, e.g. task_base_url for deep links) ──
  CREATE TABLE IF NOT EXISTS system_config (
    system_name  TEXT NOT NULL,
    config_key   TEXT NOT NULL,
    config_value TEXT,
    PRIMARY KEY (system_name, config_key)
  );

  CREATE TABLE IF NOT EXISTS inbox_items (
    id          TEXT PRIMARY KEY,
    source      TEXT NOT NULL,
    type        TEXT NOT NULL,
    category    TEXT NOT NULL DEFAULT 'action',
    title       TEXT NOT NULL,
    description TEXT,
    status      TEXT NOT NULL DEFAULT 'pending',
    priority    TEXT NOT NULL DEFAULT 'normal',
    assigned_to TEXT,
    task_path   TEXT,
    link        TEXT,
    due_date    TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
  );

  -- ── Audit & Idempotency ──
  CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    direction TEXT NOT NULL,
    topic TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_id TEXT,
    canonical_id TEXT,
    summary TEXT,
    timestamp TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL
  );

  -- ── Users & Identity ──
  CREATE TABLE IF NOT EXISTS users (
    user_id         TEXT PRIMARY KEY,
    external_id     TEXT UNIQUE,      -- IdP subject / SCIM externalId
    username        TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    email           TEXT,
    role            TEXT NOT NULL DEFAULT 'viewer',
    org_unit        TEXT,
    products        TEXT NOT NULL DEFAULT '[]',   -- JSON array
    primary_product TEXT,
    groups          TEXT NOT NULL DEFAULT '[]',   -- JSON array
    status          TEXT NOT NULL DEFAULT 'active',  -- active, suspended, deprovisioned
    source          TEXT NOT NULL DEFAULT 'local',   -- local, scim, oidc
    password_hash   TEXT,              -- only for local/demo users
    last_login      TEXT,
    synced_at       TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
  );

  -- ── Economy Domain (staging) ──
  CREATE TABLE IF NOT EXISTS econ_entities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system   TEXT NOT NULL,
    dimension       TEXT NOT NULL,
    code            TEXT NOT NULL,
    name            TEXT NOT NULL,
    type            TEXT NOT NULL DEFAULT 'leaf',
    status          TEXT NOT NULL DEFAULT 'active',
    valid_from      TEXT,
    valid_to        TEXT,
    received_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(dimension, code)
  );

  CREATE TABLE IF NOT EXISTS econ_entity_attributes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    dimension       TEXT NOT NULL,
    code            TEXT NOT NULL,
    attribute_name  TEXT NOT NULL,
    attribute_value TEXT NOT NULL,
    source_system   TEXT NOT NULL,
    received_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(dimension, code, attribute_name, source_system)
  );

  CREATE TABLE IF NOT EXISTS econ_attribute_defs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    dimension       TEXT NOT NULL,
    attribute_name  TEXT NOT NULL,
    attribute_label TEXT NOT NULL,
    data_type       TEXT NOT NULL DEFAULT 'string',
    source_system   TEXT,
    allowed_values  TEXT,
    UNIQUE(dimension, attribute_name, source_system)
  );

  CREATE TABLE IF NOT EXISTS econ_relations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system   TEXT NOT NULL,
    relation_type   TEXT NOT NULL DEFAULT 'hierarchy',
    dimension       TEXT NOT NULL,
    child_code      TEXT NOT NULL,
    parent_code     TEXT NOT NULL,
    hierarchy_name  TEXT NOT NULL DEFAULT 'standard',
    level           INTEGER NOT NULL DEFAULT 0,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    valid_from      TEXT,
    valid_to        TEXT,
    received_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(dimension, hierarchy_name, child_code, parent_code)
  );

  CREATE TABLE IF NOT EXISTS econ_facts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system     TEXT NOT NULL,
    source_batch_id   TEXT,
    source_row_id     TEXT,
    source_modified_at TEXT,
    project_id        TEXT,
    account           TEXT NOT NULL,
    org_unit          TEXT NOT NULL,
    period            TEXT NOT NULL,
    amount            REAL NOT NULL,
    currency          TEXT NOT NULL DEFAULT 'SEK',
    transaction_date  TEXT,
    dim1              TEXT,
    dim2              TEXT,
    dim3              TEXT,
    dim4              TEXT,
    dim5              TEXT,
    dim6              TEXT,
    staging_status    TEXT NOT NULL DEFAULT 'received',
    received_at       TEXT NOT NULL DEFAULT (datetime('now')),
    validated_at      TEXT
  );

  CREATE TABLE IF NOT EXISTS sync_state (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system   TEXT NOT NULL,
    entity_type     TEXT NOT NULL,
    last_sync_at    TEXT,
    high_watermark  TEXT,
    rows_received   INTEGER DEFAULT 0,
    rows_validated  INTEGER DEFAULT 0,
    rows_rejected   INTEGER DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'idle',
    schedule_cron   TEXT,
    duration_ms     INTEGER,
    UNIQUE(source_system, entity_type)
  );
`);

// ── Schema migrations (add columns to existing tables) ──
function addColumnIfNotExists(table: string, column: string, type: string) {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[];
  if (!cols.some(c => c.name === column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${type}`);
    console.log(`[MAPPER] Migration: added ${table}.${column}`);
  }
}
addColumnIfNotExists("inbox_items", "assigned_to", "TEXT");
addColumnIfNotExists("inbox_items", "task_path", "TEXT");
addColumnIfNotExists("inbox_items", "due_date", "TEXT");
addColumnIfNotExists("inbox_items", "category", "TEXT DEFAULT 'action'");
addColumnIfNotExists("dimension_code_mappings", "source_key", "TEXT");

// ── Prepared statements ──

const stmts = {
  insertCanonical: db.prepare(
    "INSERT OR IGNORE INTO canonical_projects (canonical_id) VALUES (?)"
  ),
  insertMapping: db.prepare(
    "INSERT OR REPLACE INTO id_mappings (source_system, source_id, canonical_id) VALUES (?, ?, ?)"
  ),
  findBySource: db.prepare(
    "SELECT canonical_id FROM id_mappings WHERE source_system = ? AND source_id = ?"
  ),
  findMappings: db.prepare(
    "SELECT source_system, source_id FROM id_mappings WHERE canonical_id = ?"
  ),
  allCanonicals: db.prepare(`
    SELECT cp.canonical_id, cp.created_at, im.source_system, im.source_id
    FROM canonical_projects cp
    LEFT JOIN id_mappings im ON cp.canonical_id = im.canonical_id
    ORDER BY cp.created_at
  `),
  updateMappingsCanonical: db.prepare(
    "UPDATE id_mappings SET canonical_id = ? WHERE canonical_id = ?"
  ),
  deleteCanonical: db.prepare(
    "DELETE FROM canonical_projects WHERE canonical_id = ?"
  ),
};

// ── Counter for canonical IDs ──
let counter = 0;
const maxRow = db.prepare("SELECT COUNT(*) as c FROM canonical_projects").get() as { c: number };
counter = maxRow.c;

function nextCanonicalId(): string {
  counter++;
  return `platform-${String(counter).padStart(3, "0")}`;
}

// ── Public API ──

// ── Delete helpers ──

export function deleteDimModel(product: string, slot: string) {
  db.prepare("DELETE FROM dim_models WHERE product = ? AND slot = ?").run(product, slot);
  console.log(`[MAPPER] Deleted dim model: ${product}.${slot}`);
}

export function deleteDimRouting(sourceSystem: string, sourceField: string, targetProduct: string) {
  db.prepare("DELETE FROM dim_routing WHERE source_system = ? AND source_field = ? AND target_product = ?").run(sourceSystem, sourceField, targetProduct);
  console.log(`[MAPPER] Deleted routing: ${sourceSystem}.${sourceField} → ${targetProduct}`);
}

export function deleteSystem(systemName: string) {
  db.prepare("DELETE FROM dim_routing WHERE source_system = ? OR target_product = ?").run(systemName, systemName);
  db.prepare("DELETE FROM dim_models WHERE product = ?").run(systemName);
  db.prepare("DELETE FROM dimension_participants WHERE product = ?").run(systemName);
  db.prepare("DELETE FROM system_config WHERE system_name = ?").run(systemName);
  console.log(`[MAPPER] Deleted system: ${systemName} (cascaded)`);
}

export function deleteParticipant(dimensionName: string, product: string) {
  db.prepare("DELETE FROM dimension_participants WHERE dimension_name = ? AND product = ?").run(dimensionName, product);
  console.log(`[MAPPER] Deleted participant: ${product} from ${dimensionName}`);
}

export function resetAllData() {
  const tables = [
    'processed_events', 'audit_events',
    'inbox_items', 'users',
    'sync_state', 'econ_facts', 'econ_relations', 'econ_entity_attributes', 'econ_attribute_defs', 'econ_entities',
    'system_config', 'dimension_code_mappings',
    'dimension_participants', 'shared_dimensions',
    'dim_routing', 'dim_models', 'dimension_mappings',
    'id_mappings', 'canonical_projects',
  ];
  for (const t of tables) db.prepare(`DELETE FROM ${t}`).run();
  counter = 0;
  console.log('[MAPPER] All data reset');
}

export function getOrCreateCanonical(sourceSystem: string, sourceId: string): string {
  const existing = stmts.findBySource.get(sourceSystem, sourceId) as { canonical_id: string } | undefined;
  if (existing) return existing.canonical_id;

  const canonicalId = nextCanonicalId();
  stmts.insertCanonical.run(canonicalId);
  stmts.insertMapping.run(sourceSystem, sourceId, canonicalId);
  console.log(`[MAPPER] Created ${canonicalId} for ${sourceSystem}:${sourceId}`);
  return canonicalId;
}

export function lookupCanonical(sourceSystem: string, sourceId: string): string | undefined {
  const row = stmts.findBySource.get(sourceSystem, sourceId) as { canonical_id: string } | undefined;
  return row?.canonical_id;
}

export function getMappings(canonicalId: string): Record<string, string> {
  const rows = stmts.findMappings.all(canonicalId) as { source_system: string; source_id: string }[];
  const result: Record<string, string> = {};
  for (const row of rows) {
    result[row.source_system] = row.source_id;
  }
  return result;
}

export function linkProjects(sourceId: string, targetId: string): {
  canonical_id: string;
  linked: Record<string, string>;
} {
  // Find canonical IDs for both
  const sourceCanonical = findCanonicalForAnySystem(sourceId);
  const targetCanonical = findCanonicalForAnySystem(targetId);

  if (!sourceCanonical && !targetCanonical) {
    throw new Error(`Neither ${sourceId} nor ${targetId} is known`);
  }

  // Merge: keep the first one found, move all mappings to it
  const keepId = sourceCanonical || targetCanonical!;
  const mergeId = sourceCanonical && targetCanonical && sourceCanonical !== targetCanonical
    ? targetCanonical
    : null;

  if (mergeId) {
    // Check ownership BEFORE moving mappings (mergeId's source will be gone after)
    const ownerRow = db.prepare("SELECT owner_system FROM shared_dimensions WHERE name = 'project'").get() as { owner_system: string } | undefined;
    const mergeSource = db.prepare("SELECT source_system FROM id_mappings WHERE canonical_id = ?").get(mergeId) as { source_system: string } | undefined;
    const mergeIsOwner = ownerRow && mergeSource?.source_system === ownerRow.owner_system;

    stmts.updateMappingsCanonical.run(keepId, mergeId);
    stmts.deleteCanonical.run(mergeId);
    // Merge dimension codes: update cross-references pointing at old canonical
    db.prepare("UPDATE dimension_code_mappings SET canonical_code = ? WHERE dimension_name = 'project' AND canonical_code = ?").run(keepId, mergeId);
    // Determine authoritative label: prefer the dimension owner's name
    const keepRow = db.prepare("SELECT name FROM econ_entities WHERE dimension = 'project' AND code = ?").get(keepId) as { name: string } | undefined;
    const mergeRow = db.prepare("SELECT name FROM econ_entities WHERE dimension = 'project' AND code = ?").get(mergeId) as { name: string } | undefined;
    if (mergeRow) {
      if (!keepRow) {
        db.prepare("UPDATE econ_entities SET code = ? WHERE dimension = 'project' AND code = ?").run(keepId, mergeId);
      } else {
        db.prepare("DELETE FROM econ_entities WHERE dimension = 'project' AND code = ?").run(mergeId);
      }
      // If the merged canonical came from the dimension owner, use its label
      if (mergeIsOwner) {
        db.prepare("UPDATE econ_entities SET name = ? WHERE dimension = 'project' AND code = ?").run(mergeRow.name, keepId);
      }
    }
    console.log(`[MAPPER] Merged ${mergeId} into ${keepId}`);
  }

  const linked = getMappings(keepId);
  return { canonical_id: keepId, linked };
}

function findCanonicalForAnySystem(id: string): string | undefined {
  for (const sys of ["erp", "prod_a", "prod_b"]) {
    const found = lookupCanonical(sys, id);
    if (found) return found;
  }
  return undefined;
}

export function getAllProjects() {
  const rows = stmts.allCanonicals.all() as {
    canonical_id: string;
    created_at: string;
    source_system: string | null;
    source_id: string | null;
  }[];

  const projects = new Map<string, { canonical_id: string; created_at: string; mappings: Record<string, string> }>();
  for (const row of rows) {
    if (!projects.has(row.canonical_id)) {
      projects.set(row.canonical_id, {
        canonical_id: row.canonical_id,
        created_at: row.created_at,
        mappings: {},
      });
    }
    if (row.source_system && row.source_id) {
      projects.get(row.canonical_id)!.mappings[row.source_system] = row.source_id;
    }
  }
  return Array.from(projects.values());
}

// ── Planning-dimension mapping ──
// Translates Product A's version concept ("Budget 2025") into Product B's
// richer dimensional model (planning_year + planning_type + planning_version).

function parseVersionName(versionName: string, year: string): { planning_type: string; planning_year: string; planning_version: number } {
  // "Budget 2025" → Budget, "F1 2025" → F1, "F2 2026" → F2, etc.
  const match = versionName.match(/^(Budget|F\d+)\s+(\d{4})/i);
  if (match) {
    return { planning_type: match[1], planning_year: match[2], planning_version: 1 };
  }
  return { planning_type: "Budget", planning_year: year, planning_version: 1 };
}

export function getOrCreateDimensionMapping(
  canonicalId: string,
  versionName: string,
  year: string,
  sourceVersionId?: string
): { planning_year: string; planning_type: string; planning_version: number } {
  // Check if an explicit mapping already exists for this canonical + version
  const existing = db.prepare(
    "SELECT planning_year, planning_type, planning_version FROM dimension_mappings WHERE canonical_id = ? AND source_version_id = ?"
  ).get(canonicalId, sourceVersionId || null) as { planning_year: string; planning_type: string; planning_version: number } | undefined;

  if (existing) return existing;

  // Auto-create based on convention
  const parsed = parseVersionName(versionName, year);

  // Find next planning_version if same type+year already exists
  const maxVer = db.prepare(
    "SELECT MAX(planning_version) as mv FROM dimension_mappings WHERE canonical_id = ? AND planning_year = ? AND planning_type = ?"
  ).get(canonicalId, parsed.planning_year, parsed.planning_type) as { mv: number | null };
  const nextVersion = (maxVer?.mv ?? 0) + 1;

  db.prepare(
    "INSERT OR IGNORE INTO dimension_mappings (canonical_id, source_version_id, planning_year, planning_type, planning_version) VALUES (?, ?, ?, ?, ?)"
  ).run(canonicalId, sourceVersionId || null, parsed.planning_year, parsed.planning_type, nextVersion);

  console.log(`[MAPPER] Dimension mapping: "${versionName}" → ${parsed.planning_type} ${parsed.planning_year} v${nextVersion} (${canonicalId})`);
  return { planning_year: parsed.planning_year, planning_type: parsed.planning_type, planning_version: nextVersion };
}

export function getDimensionMappings(canonicalId: string) {
  return db.prepare(
    "SELECT * FROM dimension_mappings WHERE canonical_id = ? ORDER BY planning_year, planning_type, planning_version"
  ).all(canonicalId);
}

export function getAllDimensionMappings() {
  return db.prepare(
    "SELECT dm.*, cp.created_at AS project_created_at FROM dimension_mappings dm LEFT JOIN canonical_projects cp ON dm.canonical_id = cp.canonical_id ORDER BY dm.created_at DESC"
  ).all();
}

export function updateDimensionMapping(id: number, updates: { planning_year?: string; planning_type?: string; planning_version?: number }) {
  const current = db.prepare("SELECT * FROM dimension_mappings WHERE id = ?").get(id) as any;
  if (!current) throw new Error(`Dimension mapping ${id} not found`);

  const newYear = updates.planning_year || current.planning_year;
  const newType = updates.planning_type || current.planning_type;
  const newVersion = updates.planning_version ?? current.planning_version;

  db.prepare(
    "UPDATE dimension_mappings SET planning_year = ?, planning_type = ?, planning_version = ? WHERE id = ?"
  ).run(newYear, newType, newVersion, id);

  console.log(`[MAPPER] Dimension mapping #${id} updated: ${newType} ${newYear} v${newVersion}`);
  return { id, canonical_id: current.canonical_id, source_version_id: current.source_version_id, planning_year: newYear, planning_type: newType, planning_version: newVersion };
}

// ── Flex-dimension model & routing ──

export function configureDimModel(product: string, slot: string, label: string) {
  db.prepare(
    "INSERT OR REPLACE INTO dim_models (product, slot, label) VALUES (?, ?, ?)"
  ).run(product, slot, label);
  console.log(`[MAPPER] Dim model: ${product}.${slot} = "${label}"`);
}

export function getDimModel(product: string): Record<string, string> {
  const rows = db.prepare("SELECT slot, label FROM dim_models WHERE product = ?").all(product) as { slot: string; label: string }[];
  const result: Record<string, string> = {};
  for (const r of rows) result[r.slot] = r.label;
  return result;
}

export function getAllDimModels() {
  return db.prepare("SELECT * FROM dim_models ORDER BY product, slot").all();
}

export function configureDimRouting(sourceSystem: string, sourceField: string, targetProduct: string, targetSlot: string) {
  db.prepare(
    "INSERT OR REPLACE INTO dim_routing (source_system, source_field, target_product, target_slot) VALUES (?, ?, ?, ?)"
  ).run(sourceSystem, sourceField, targetProduct, targetSlot);
  console.log(`[MAPPER] Dim routing: ${sourceSystem}.${sourceField} → ${targetProduct}.${targetSlot}`);
}

export function getDimRouting(sourceSystem: string, targetProduct: string): Array<{ source_field: string; target_slot: string }> {
  return db.prepare(
    "SELECT source_field, target_slot FROM dim_routing WHERE source_system = ? AND target_product = ?"
  ).all(sourceSystem, targetProduct) as Array<{ source_field: string; target_slot: string }>;
}

export function getAllDimRouting() {
  return db.prepare("SELECT dr.*, dm.label FROM dim_routing dr LEFT JOIN dim_models dm ON dr.target_product = dm.product AND dr.target_slot = dm.slot ORDER BY dr.source_system, dr.source_field").all();
}

export function applyDimRouting(sourceSystem: string, targetProduct: string, sourceData: Record<string, unknown>): Record<string, string | null> {
  const routing = getDimRouting(sourceSystem, targetProduct);
  const result: Record<string, string | null> = { dim1: null, dim2: null, dim3: null, dim4: null, dim5: null };
  for (const r of routing) {
    const value = sourceData[r.source_field];
    if (value !== undefined && value !== null) {
      // Try code translation: source system's local code → canonical code
      const translated = translateCode(sourceSystem, r.source_field, String(value));
      result[r.target_slot] = translated;
    }
  }
  return result;
}

// Translate a source system's local code to canonical code via dimension_code_mappings.
// Falls back to the original value if no mapping exists (passthrough).
function translateCode(sourceSystem: string, fieldName: string, localCode: string): string {
  // Look up mapping scoped to dimension_name to avoid cross-dimension collisions
  const row = db.prepare(
    "SELECT canonical_code FROM dimension_code_mappings WHERE dimension_name = ? AND product = ? AND local_code = ? LIMIT 1"
  ).get(fieldName, sourceSystem, localCode) as { canonical_code: string } | undefined;
  if (row) return row.canonical_code;
  // Fallback: try without dimension_name for backwards compatibility
  const fallback = db.prepare(
    "SELECT canonical_code FROM dimension_code_mappings WHERE product = ? AND local_code = ? LIMIT 1"
  ).get(sourceSystem, localCode) as { canonical_code: string } | undefined;
  return fallback ? fallback.canonical_code : localCode;
}

// ── Shared Dimension Catalog ──

export function registerSharedDimension(name: string, label: string, ownerSystem: string, taxonomyType: string = "shared", dimensionType: string = "flat") {
  db.prepare(
    "INSERT OR REPLACE INTO shared_dimensions (name, label, owner_system, taxonomy_type, dimension_type) VALUES (?, ?, ?, ?, ?)"
  ).run(name, label, ownerSystem, taxonomyType, dimensionType);
  console.log(`[MAPPER] Shared dimension: ${name} (${label}), owner=${ownerSystem}, type=${taxonomyType}, dim_type=${dimensionType}`);
}

export function getAllSharedDimensions() {
  const dims = db.prepare("SELECT * FROM shared_dimensions ORDER BY name").all() as any[];
  return dims.map(d => {
    const participants = db.prepare("SELECT product, role, uses_canonical FROM dimension_participants WHERE dimension_name = ?").all(d.name);
    const codeCount = (db.prepare("SELECT COUNT(*) as c FROM econ_entities WHERE dimension = ?").get(d.name) as { c: number }).c;
    const attributes = db.prepare("SELECT DISTINCT attribute_name, attribute_label, data_type FROM econ_attribute_defs WHERE dimension = ?").all(d.name);
    const hierarchyCount = (db.prepare("SELECT COUNT(*) as c FROM econ_relations WHERE dimension = ?").get(d.name) as { c: number }).c;
    return { ...d, participants, code_count: codeCount, attributes, hierarchy_count: hierarchyCount };
  });
}

// ── Shared Dimension Codes — delegate to Economy Domain ──

export function upsertDimensionCode(dimensionName: string, code: string, label: string) {
  // Delegate to econ_entities — the economy domain is now source of truth
  upsertEconEntity({ source_system: "platform", dimension: dimensionName, code, name: label });
}

export function deleteDimensionCode(dimensionName: string, code: string) {
  deleteEconEntity(dimensionName, code);
  db.prepare("DELETE FROM dimension_code_mappings WHERE dimension_name = ? AND canonical_code = ?").run(dimensionName, code);
}

export function getDimensionCodes(dimensionName: string) {
  // Return in old format: { id, dimension_name, code, label, created_at }
  return db.prepare("SELECT id, dimension as dimension_name, code, name as label, received_at as created_at FROM econ_entities WHERE dimension = ? ORDER BY code").all(dimensionName);
}

// ── Dimension Attributes & Hierarchy — delegate to Economy Domain ──

export function registerDimensionAttribute(dimensionName: string, attributeName: string, attributeLabel: string, dataType: string = "string") {
  upsertEconAttributeDef({ dimension: dimensionName, attribute_name: attributeName, attribute_label: attributeLabel, data_type: dataType, source_system: "platform" });
}

export function getDimensionAttributes(dimensionName: string) {
  return db.prepare("SELECT dimension as dimension_name, attribute_name, attribute_label, data_type FROM econ_attribute_defs WHERE dimension = ? ORDER BY attribute_name").all(dimensionName);
}

export function setCodeAttribute(dimensionName: string, code: string, attributeName: string, value: string) {
  upsertEconEntityAttribute({ dimension: dimensionName, code, attribute_name: attributeName, attribute_value: value, source_system: "platform" });
}

export function getCodeAttributes(dimensionName: string, code: string) {
  return db.prepare(
    "SELECT attribute_name, attribute_value as value FROM econ_entity_attributes WHERE dimension = ? AND code = ?"
  ).all(dimensionName, code) as Array<{ attribute_name: string; value: string }>;
}

export function getAllCodeAttributes(dimensionName: string) {
  return db.prepare(
    "SELECT code, attribute_name, attribute_value as value FROM econ_entity_attributes WHERE dimension = ? ORDER BY code, attribute_name"
  ).all(dimensionName);
}

export function setHierarchy(dimensionName: string, childCode: string, parentCode: string, level: number = 0) {
  upsertEconRelation({ source_system: "platform", dimension: dimensionName, child_code: childCode, parent_code: parentCode, hierarchy_name: "standard", level });
}

export function getHierarchy(dimensionName: string) {
  return db.prepare(
    "SELECT child_code, parent_code, level FROM econ_relations WHERE dimension = ? ORDER BY level, parent_code, child_code"
  ).all(dimensionName);
}

export function registerParticipant(dimensionName: string, product: string, role: string, usesCanonical: boolean = true) {
  db.prepare(
    "INSERT OR REPLACE INTO dimension_participants (dimension_name, product, role, uses_canonical) VALUES (?, ?, ?, ?)"
  ).run(dimensionName, product, role, usesCanonical ? 1 : 0);
  console.log(`[MAPPER] Participant: ${product} is ${role} of ${dimensionName} (canonical=${usesCanonical})`);
}

export function getParticipants(dimensionName: string) {
  return db.prepare("SELECT product, role, uses_canonical FROM dimension_participants WHERE dimension_name = ? ORDER BY product").all(dimensionName);
}

export function upsertCodeMapping(dimensionName: string, product: string, localCode: string, canonicalCode: string, sourceKey?: string) {
  db.prepare(
    "INSERT OR REPLACE INTO dimension_code_mappings (dimension_name, product, local_code, canonical_code, source_key) VALUES (?, ?, ?, ?, ?)"
  ).run(dimensionName, product, localCode, canonicalCode, sourceKey || null);
}

// ── Inbox ──

export function getInboxItems(assignedTo?: string, status?: string): any[] {
  let sql = "SELECT * FROM inbox_items WHERE 1=1";
  const params: any[] = [];
  if (assignedTo) { sql += " AND (assigned_to = ? OR assigned_to IS NULL)"; params.push(assignedTo); }
  if (status) { sql += " AND status = ?"; params.push(status); }
  sql += " ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END, created_at DESC";
  return db.prepare(sql).all(...params);
}

export function addInboxItem(item: { id: string; source: string; type: string; title: string; description?: string; priority?: string; link?: string; assigned_to?: string; task_path?: string; due_date?: string; category?: string }) {
  db.prepare(
    "INSERT OR REPLACE INTO inbox_items (id, source, type, category, title, description, priority, assigned_to, task_path, link, due_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  ).run(item.id, item.source, item.type, item.category || 'action', item.title, item.description || null, item.priority || 'normal', item.assigned_to || null, item.task_path || null, item.link || null, item.due_date || null);
}

export function updateInboxItem(id: string, updates: { status?: string; title?: string; description?: string }) {
  if (updates.status) {
    db.prepare("UPDATE inbox_items SET status = ?, updated_at = datetime('now') WHERE id = ?").run(updates.status, id);
  }
}

// ── System Config (key-value per system) ──

export function setSystemConfig(systemName: string, key: string, value: string | null) {
  db.prepare("INSERT OR REPLACE INTO system_config (system_name, config_key, config_value) VALUES (?, ?, ?)").run(systemName, key, value);
}

export function getSystemConfig(systemName: string, key: string): string | null {
  const row = db.prepare("SELECT config_value FROM system_config WHERE system_name = ? AND config_key = ?").get(systemName, key) as { config_value: string | null } | undefined;
  return row?.config_value || null;
}

export function getAllSystemConfigs(): any[] {
  return db.prepare("SELECT * FROM system_config ORDER BY system_name, config_key").all();
}

export function getCodeMappings(dimensionName: string, product?: string) {
  if (product) {
    return db.prepare("SELECT * FROM dimension_code_mappings WHERE dimension_name = ? AND product = ? ORDER BY local_code").all(dimensionName, product);
  }
  return db.prepare("SELECT * FROM dimension_code_mappings WHERE dimension_name = ? ORDER BY product, local_code").all(dimensionName);
}



// ── Users & Identity ──

export interface UserRecord {
  user_id: string;
  external_id?: string | null;
  username: string;
  name: string;
  email?: string | null;
  role: string;
  org_unit?: string | null;
  products: string[];
  primary_product?: string | null;
  groups: string[];
  status: string;
  source: string;
  password_hash?: string | null;
  last_login?: string | null;
  synced_at?: string | null;
  created_at?: string;
  updated_at?: string;
}

function rowToUser(row: any): UserRecord {
  return {
    ...row,
    products: JSON.parse(row.products || "[]"),
    groups: JSON.parse(row.groups || "[]"),
  };
}

export function getAllUsers(): UserRecord[] {
  const rows = db.prepare("SELECT * FROM users ORDER BY user_id").all() as any[];
  return rows.map(rowToUser);
}

export function getUser(userId: string): UserRecord | undefined {
  const row = db.prepare("SELECT * FROM users WHERE user_id = ?").get(userId) as any;
  return row ? rowToUser(row) : undefined;
}

export function getUserByUsername(username: string): UserRecord | undefined {
  const row = db.prepare("SELECT * FROM users WHERE username = ? AND status = 'active'").get(username) as any;
  return row ? rowToUser(row) : undefined;
}

export function getUserByExternalId(externalId: string): UserRecord | undefined {
  const row = db.prepare("SELECT * FROM users WHERE external_id = ?").get(externalId) as any;
  return row ? rowToUser(row) : undefined;
}

export function upsertUser(user: {
  user_id: string;
  external_id?: string;
  username: string;
  name: string;
  email?: string;
  role?: string;
  org_unit?: string;
  products?: string[];
  primary_product?: string;
  groups?: string[];
  status?: string;
  source?: string;
  password_hash?: string;
}): UserRecord {
  db.prepare(`
    INSERT INTO users (user_id, external_id, username, name, email, role, org_unit, products, primary_product, groups, status, source, password_hash, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(user_id) DO UPDATE SET
      external_id = excluded.external_id,
      username = excluded.username,
      name = excluded.name,
      email = excluded.email,
      role = excluded.role,
      org_unit = excluded.org_unit,
      products = excluded.products,
      primary_product = excluded.primary_product,
      groups = excluded.groups,
      status = excluded.status,
      source = excluded.source,
      password_hash = CASE WHEN excluded.password_hash IS NOT NULL THEN excluded.password_hash ELSE users.password_hash END,
      synced_at = datetime('now'),
      updated_at = datetime('now')
  `).run(
    user.user_id,
    user.external_id || null,
    user.username,
    user.name,
    user.email || null,
    user.role || "viewer",
    user.org_unit || null,
    JSON.stringify(user.products || []),
    user.primary_product || null,
    JSON.stringify(user.groups || []),
    user.status || "active",
    user.source || "local",
    user.password_hash || null,
  );
  return getUser(user.user_id)!;
}

export function updateUser(userId: string, updates: Partial<{
  name: string;
  email: string;
  role: string;
  org_unit: string;
  products: string[];
  primary_product: string;
  groups: string[];
  status: string;
}>): UserRecord | undefined {
  const user = getUser(userId);
  if (!user) return undefined;
  const sets: string[] = [];
  const params: any[] = [];
  if (updates.name !== undefined) { sets.push("name = ?"); params.push(updates.name); }
  if (updates.email !== undefined) { sets.push("email = ?"); params.push(updates.email); }
  if (updates.role !== undefined) { sets.push("role = ?"); params.push(updates.role); }
  if (updates.org_unit !== undefined) { sets.push("org_unit = ?"); params.push(updates.org_unit); }
  if (updates.products !== undefined) { sets.push("products = ?"); params.push(JSON.stringify(updates.products)); }
  if (updates.primary_product !== undefined) { sets.push("primary_product = ?"); params.push(updates.primary_product); }
  if (updates.groups !== undefined) { sets.push("groups = ?"); params.push(JSON.stringify(updates.groups)); }
  if (updates.status !== undefined) { sets.push("status = ?"); params.push(updates.status); }
  if ((updates as any).password_hash !== undefined) { sets.push("password_hash = ?"); params.push((updates as any).password_hash); }
  if (sets.length === 0) return user;
  sets.push("updated_at = datetime('now')");
  params.push(userId);
  db.prepare(`UPDATE users SET ${sets.join(", ")} WHERE user_id = ?`).run(...params);
  return getUser(userId);
}

export function deleteUser(userId: string): boolean {
  const result = db.prepare("DELETE FROM users WHERE user_id = ?").run(userId);
  return result.changes > 0;
}

export function updateLastLogin(userId: string) {
  db.prepare("UPDATE users SET last_login = datetime('now') WHERE user_id = ?").run(userId);
}

export function getUserCount(): number {
  return (db.prepare("SELECT COUNT(*) as c FROM users").get() as { c: number }).c;
}

// ── Audit Events ──

export function insertAuditEvent(direction: 'in' | 'out', topic: string, event_type: string, event_id: string | undefined, canonical_id: string | undefined, summary: string) {
  db.prepare(
    "INSERT INTO audit_events (direction, topic, event_type, event_id, canonical_id, summary, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)"
  ).run(direction, topic, event_type, event_id || null, canonical_id || null, summary, new Date().toISOString());
}

export function getAuditEvents(limit = 100): Array<{ id: number; direction: string; topic: string; event_type: string; event_id: string | null; canonical_id: string | null; summary: string; timestamp: string }> {
  return db.prepare("SELECT * FROM audit_events ORDER BY id DESC LIMIT ?").all(limit) as any;
}

export function getAuditEventCount(): number {
  return (db.prepare("SELECT COUNT(*) as c FROM audit_events").get() as { c: number }).c;
}

// ── Idempotency ──

export function isEventProcessed(eventId: string): boolean {
  return !!db.prepare("SELECT 1 FROM processed_events WHERE event_id = ?").get(eventId);
}

export function markEventProcessed(eventId: string) {
  db.prepare("INSERT OR IGNORE INTO processed_events (event_id, processed_at) VALUES (?, ?)").run(eventId, new Date().toISOString());
}

// ── Economy Domain ──

export function upsertEconEntity(entity: { source_system: string; dimension: string; code: string; name: string; type?: string; status?: string; valid_from?: string; valid_to?: string }) {
  db.prepare(`
    INSERT INTO econ_entities (source_system, dimension, code, name, type, status, valid_from, valid_to, received_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(dimension, code) DO UPDATE SET
      source_system = excluded.source_system, name = excluded.name, type = excluded.type,
      status = excluded.status, valid_from = excluded.valid_from, valid_to = excluded.valid_to,
      received_at = datetime('now')
  `).run(entity.source_system, entity.dimension, entity.code, entity.name, entity.type || "leaf", entity.status || "active", entity.valid_from || null, entity.valid_to || null);
}

export function getEconEntities(dimension?: string): any[] {
  if (dimension) return db.prepare("SELECT * FROM econ_entities WHERE dimension = ? ORDER BY code").all(dimension);
  return db.prepare("SELECT * FROM econ_entities ORDER BY dimension, code").all();
}

export function deleteEconEntity(dimension: string, code: string): boolean {
  db.prepare("DELETE FROM econ_entity_attributes WHERE dimension = ? AND code = ?").run(dimension, code);
  db.prepare("DELETE FROM econ_relations WHERE dimension = ? AND (child_code = ? OR parent_code = ?)").run(dimension, code, code);
  return db.prepare("DELETE FROM econ_entities WHERE dimension = ? AND code = ?").run(dimension, code).changes > 0;
}

export function upsertEconEntityAttribute(attr: { dimension: string; code: string; attribute_name: string; attribute_value: string; source_system: string }) {
  db.prepare(`
    INSERT INTO econ_entity_attributes (dimension, code, attribute_name, attribute_value, source_system, received_at)
    VALUES (?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(dimension, code, attribute_name, source_system) DO UPDATE SET
      attribute_value = excluded.attribute_value, received_at = datetime('now')
  `).run(attr.dimension, attr.code, attr.attribute_name, attr.attribute_value, attr.source_system);
}

export function getEconEntityAttributes(dimension: string, code: string): any[] {
  return db.prepare("SELECT * FROM econ_entity_attributes WHERE dimension = ? AND code = ? ORDER BY source_system, attribute_name").all(dimension, code);
}

export function upsertEconAttributeDef(def: { dimension: string; attribute_name: string; attribute_label: string; data_type?: string; source_system?: string; allowed_values?: string }) {
  db.prepare(`
    INSERT INTO econ_attribute_defs (dimension, attribute_name, attribute_label, data_type, source_system, allowed_values)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(dimension, attribute_name, source_system) DO UPDATE SET
      attribute_label = excluded.attribute_label, data_type = excluded.data_type, allowed_values = excluded.allowed_values
  `).run(def.dimension, def.attribute_name, def.attribute_label, def.data_type || "string", def.source_system || null, def.allowed_values || null);
}

export function getEconAttributeDefs(dimension?: string): any[] {
  if (dimension) return db.prepare("SELECT * FROM econ_attribute_defs WHERE dimension = ? ORDER BY attribute_name").all(dimension);
  return db.prepare("SELECT * FROM econ_attribute_defs ORDER BY dimension, attribute_name").all();
}

export function upsertEconRelation(rel: { source_system: string; relation_type?: string; dimension: string; child_code: string; parent_code: string; hierarchy_name?: string; level?: number; sort_order?: number; valid_from?: string; valid_to?: string }) {
  db.prepare(`
    INSERT INTO econ_relations (source_system, relation_type, dimension, child_code, parent_code, hierarchy_name, level, sort_order, valid_from, valid_to, received_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(dimension, hierarchy_name, child_code, parent_code) DO UPDATE SET
      source_system = excluded.source_system, relation_type = excluded.relation_type,
      level = excluded.level, sort_order = excluded.sort_order,
      valid_from = excluded.valid_from, valid_to = excluded.valid_to, received_at = datetime('now')
  `).run(rel.source_system, rel.relation_type || "hierarchy", rel.dimension, rel.child_code, rel.parent_code, rel.hierarchy_name || "standard", rel.level ?? 0, rel.sort_order ?? 0, rel.valid_from || null, rel.valid_to || null);
}

export function getEconRelations(dimension?: string, hierarchyName?: string): any[] {
  if (dimension && hierarchyName) return db.prepare("SELECT * FROM econ_relations WHERE dimension = ? AND hierarchy_name = ? ORDER BY level, sort_order").all(dimension, hierarchyName);
  if (dimension) return db.prepare("SELECT * FROM econ_relations WHERE dimension = ? ORDER BY hierarchy_name, level, sort_order").all(dimension);
  return db.prepare("SELECT * FROM econ_relations ORDER BY dimension, hierarchy_name, level, sort_order").all();
}

export function insertEconFacts(facts: Array<{ source_system: string; source_batch_id?: string; source_row_id?: string; source_modified_at?: string; project_id?: string; account: string; org_unit: string; period: string; amount: number; currency?: string; transaction_date?: string; dim1?: string; dim2?: string; dim3?: string; dim4?: string; dim5?: string; dim6?: string }>): { received: number; rejected: number; batch_id: string } {
  const batchId = facts[0]?.source_batch_id || `batch-${Date.now()}`;
  const stmt = db.prepare(`
    INSERT INTO econ_facts (source_system, source_batch_id, source_row_id, source_modified_at, project_id, account, org_unit, period, amount, currency, transaction_date, dim1, dim2, dim3, dim4, dim5, dim6, staging_status, received_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'received', datetime('now'))
  `);
  let received = 0, rejected = 0;
  const run = db.transaction(() => {
    for (const f of facts) {
      try {
        stmt.run(f.source_system, batchId, f.source_row_id || null, f.source_modified_at || null, f.project_id || null, f.account, f.org_unit, f.period, f.amount, f.currency || "SEK", f.transaction_date || null, f.dim1 || null, f.dim2 || null, f.dim3 || null, f.dim4 || null, f.dim5 || null, f.dim6 || null);
        received++;
      } catch { rejected++; }
    }
  });
  run();
  return { received, rejected, batch_id: batchId };
}

export function validateEconFacts(batchId?: string): { validated: number; rejected: number; errors: string[] } {
  const where = batchId ? "AND source_batch_id = ?" : "";
  const params = batchId ? ["received", batchId] : ["received"];
  const pending = db.prepare(`SELECT * FROM econ_facts WHERE staging_status = ? ${where}`).all(...params) as any[];
  let validated = 0, rejected = 0;
  const errors: string[] = [];
  for (const row of pending) {
    const accExists = db.prepare("SELECT 1 FROM econ_entities WHERE dimension = 'account' AND code = ?").get(row.account);
    const orgExists = db.prepare("SELECT 1 FROM econ_entities WHERE dimension = 'org_unit' AND code = ?").get(row.org_unit);
    if (accExists && orgExists) {
      db.prepare("UPDATE econ_facts SET staging_status = 'validated', validated_at = datetime('now') WHERE id = ?").run(row.id);
      validated++;
    } else {
      db.prepare("UPDATE econ_facts SET staging_status = 'rejected' WHERE id = ?").run(row.id);
      rejected++;
      if (!accExists) errors.push(`Row ${row.id}: unknown account '${row.account}'`);
      if (!orgExists) errors.push(`Row ${row.id}: unknown org_unit '${row.org_unit}'`);
    }
  }
  return { validated, rejected, errors };
}

export function getEconFacts(opts?: { status?: string; project_id?: string; limit?: number }): any[] {
  let sql = "SELECT * FROM econ_facts WHERE 1=1";
  const params: any[] = [];
  if (opts?.status) { sql += " AND staging_status = ?"; params.push(opts.status); }
  if (opts?.project_id) { sql += " AND project_id = ?"; params.push(opts.project_id); }
  sql += " ORDER BY period, account, org_unit";
  if (opts?.limit) { sql += " LIMIT ?"; params.push(opts.limit); }
  return db.prepare(sql).all(...params);
}

export function getEconFactsSummary(): { total: number; received: number; validated: number; rejected: number; published: number } {
  const rows = db.prepare("SELECT staging_status, COUNT(*) as cnt FROM econ_facts GROUP BY staging_status").all() as { staging_status: string; cnt: number }[];
  const result = { total: 0, received: 0, validated: 0, rejected: 0, published: 0 };
  for (const r of rows) { (result as any)[r.staging_status] = r.cnt; result.total += r.cnt; }
  return result;
}

export function publishEconFacts(): number {
  const result = db.prepare("UPDATE econ_facts SET staging_status = 'published' WHERE staging_status = 'validated'").run();
  return result.changes;
}

export function getEconFactsForPublish(): any[] {
  return db.prepare("SELECT * FROM econ_facts WHERE staging_status = 'validated' ORDER BY period, account").all();
}

// Sync state management
export function upsertSyncState(source: string, entityType: string, updates: Partial<{ last_sync_at: string; high_watermark: string; rows_received: number; rows_validated: number; rows_rejected: number; status: string; schedule_cron: string; duration_ms: number }>) {
  db.prepare(`
    INSERT INTO sync_state (source_system, entity_type, status) VALUES (?, ?, 'idle')
    ON CONFLICT(source_system, entity_type) DO NOTHING
  `).run(source, entityType);
  const sets: string[] = [];
  const params: any[] = [];
  for (const [k, v] of Object.entries(updates)) { if (v !== undefined) { sets.push(`${k} = ?`); params.push(v); } }
  if (sets.length > 0) {
    params.push(source, entityType);
    db.prepare(`UPDATE sync_state SET ${sets.join(", ")} WHERE source_system = ? AND entity_type = ?`).run(...params);
  }
}

export function getSyncStates(): any[] {
  return db.prepare("SELECT * FROM sync_state ORDER BY source_system, entity_type").all();
}

export function getSyncState(source: string, entityType: string): any {
  return db.prepare("SELECT * FROM sync_state WHERE source_system = ? AND entity_type = ?").get(source, entityType);
}

export default db;
