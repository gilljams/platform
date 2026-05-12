# Platform Layer — Strategic Requirements & Phased Roadmap

> **Purpose:** Concise summary of what the platform layer requires short-term and long-term, with a phased plan to investigate, evaluate, and implement incrementally. Based on concepts proven in the Platform POC.

---

## 1. Core Requirements by Domain

### A. Economy Domain (Shared Data Model)

| # | Requirement | POC Status |
|---|---|---|
| A1 | **Canonical staging model** — facts, dimensions, structures, attributes in a source-agnostic format | ✅ Proven |
| A2 | **Adapter pattern** — per-source adapters transform external data to canonical format; platform never exposes ERP internals | ✅ Proven (single ERP) |
| A3 | **Fact pipeline** — received → validated → published, with referential integrity checks against dimension master data | ✅ Proven |
| A4 | **Idempotent sync** — upsert semantics, high watermark tracking, period-based re-read, content-hash change detection | ✅ Proven |
| A5 | **Error policy** — configurable strictness (reject / warn / skip) per source; auto-revalidate (quarantine pattern) | ✅ Proven |
| A6 | **Dimension management** — entities, hierarchies, attributes, attribute definitions; structural policies (auto_root, auto_missing, grouping rules) | ✅ Proven |
| A7 | **Attribute pipeline** — source declares attributes in capabilities; adapter discovers, extracts, routes them to correct dim-slots dynamically | ✅ Proven |
| A8 | **Multi-source** — multiple ERPs / GL sources feeding the same Economy Domain simultaneously | ⬜ Design only |
| A9 | **Multi-tenant** — tenant-isolated data with shared schema | ⬜ Not started |
| A10 | **Historical versions** — dimension snapshots with point-in-time queries; fact period locking | ⬜ Not started |

### B. Integration & Event Routing

| # | Requirement | POC Status |
|---|---|---|
| B1 | **Event-driven routing** — ingress topics (per source) → platform enrichment → egress topics (per capability) | ✅ Proven |
| B2 | **Canonical ID mapping** — platform owns cross-system identity; products never see each other's IDs | ✅ Proven |
| B3 | **Dimension routing** — source fields mapped to dim-slots via configurable `dim_routing` + code mappings | ✅ Proven |
| B4 | **Dimension snapshot publishing** — publish entity+relation snapshots to downstream products with change detection | ✅ Proven |
| B5 | **Schema contracts** — typed event schemas (Avro/Protobuf) with registry, version compatibility checks | ⬜ Not started |
| B6 | **Guaranteed delivery** — dead letter queue, retry policies, at-least-once with idempotent consumers | 🔶 Partial (DLQ exists) |
| B7 | **Adapter SDK** — standardized adapter interface so new source connectors can be developed independently | ⬜ Not started |

### C. Identity, Access & Multi-Tenancy

| # | Requirement | POC Status |
|---|---|---|
| C1 | **User provisioning** — SCIM 2.0 endpoint for IdP-driven user lifecycle | ✅ Proven |
| C2 | **Authentication** — OIDC/SAML2 redirect to external IdP; JWT token with claims | 🔶 Simulated (direct login) |
| C3 | **Product entitlements** — user → product mapping; shell navigation filtered by access | ✅ Proven |
| C4 | **Role-based access control** — coarse roles (controller, analyst, admin) | ✅ Basic |
| C5 | **Fine-grained authorization** — per-dimension, per-org-unit, per-entity permissions | ⬜ Not started |
| C6 | **Multi-tenant isolation** — tenant context in JWT; all queries scoped by tenant | ⬜ Not started |
| C7 | **API key / service account** — system-to-system auth for adapters and headless integrations | ⬜ Not started |

### D. Platform Portal (Admin UI)

| # | Requirement | POC Status |
|---|---|---|
| D1 | **Configuration management** — systems, dim models, dim routing, shared dimensions, code mappings | ✅ Proven |
| D2 | **Economy Domain overview** — entities, relations, attributes, facts, sync state, pipeline health | ✅ Proven |
| D3 | **Structural policy editor** — auto_root, auto_missing, grouping rules with strategy picker | ✅ Proven |
| D4 | **Attribute publishing rules** — map, rename, filter, enable/disable per dimension attribute | ✅ Proven |
| D5 | **Event log & DLQ** — real-time audit trail, dead letter inspection, retry | ✅ Proven |
| D6 | **Help service** — shared knowledge base with Markdown articles, categories, deep-linking, AI chat | ✅ Proven |
| D7 | **Multi-tenant portal** — tenant selector, tenant-scoped views, tenant onboarding wizard | ⬜ Not started |
| D8 | **Proper frontend framework** — replace single-file HTML with a component-based SPA (React/Vue) | ⬜ Not started |

### E. Shell (Cross-Product Experience)

| # | Requirement | POC Status |
|---|---|---|
| E1 | **Injected shell** — single JS file that renders shared navigation, inbox, help across all products | ✅ Proven |
| E2 | **Product navigation** — filtered by user entitlements; external tool links | ✅ Proven |
| E3 | **Inbox / task notifications** — cross-product task feed with badge count | ✅ Proven |
| E4 | **Help panel** — search, categories, deep-linking from product UIs | ✅ Proven |
| E5 | **AI assistant** — RAG-powered chat with help article retrieval | ✅ Proven (basic) |
| E6 | **Theming / white-label** — customer logo, colors, configurable per tenant | 🔶 Basic (logo only) |
| E7 | **Bootstrap endpoint** — single `/api/shell/bootstrap` call replaces 4 individual fetches (nav, inbox, config, help) | ✅ Proven |
| E8 | **CSP compliance** — no inline event handlers; all listeners attached via addEventListener | ✅ Proven |
| E9 | **Error isolation** — shell IIFE wrapped in try/catch; host product never breaks if shell errors | ✅ Proven |
| E10 | **Configurable platform URL** — auto-detected from script src or explicit `data-platform-url` attribute | ✅ Proven |
| E11 | **Micro-frontend integration** — shell as module federation host; products as remotes | ⬜ Not started |

---

## 2. Phased Approach — Investigate → Evaluate → Implement

### Phase 0 — Foundation Decisions (investigate)
*Before writing production code, establish the technical foundation.*

| Area | Key Questions to Resolve |
|---|---|
| **Language / Runtime** | Stay with Node.js/TypeScript or move to .NET/Go/Java for backend services? Evaluate team competence, existing product codebases, performance profiles. |
| **Database** | SQLite → PostgreSQL? Schema-per-tenant vs database-per-tenant? Shared vs dedicated? |
| **Message broker** | Redpanda/Kafka confirmed, or evaluate alternatives (RabbitMQ, Azure Service Bus, Pulsar)? Managed vs self-hosted? |
| **Deployment** | Kubernetes? Cloud-native (AKS/EKS)? On-prem support required? |
| **Frontend** | Single SPA with React/Vue, or keep shell injection pattern? Admin portal = separate app or embedded? |
| **Multi-tenancy model** | Shared infra (all tenants in one DB) vs siloed (separate DB/schema per tenant)? Impacts everything. |
| **Auth infrastructure** | Which IdP to standardize on? Zitadel, Azure AD, Okta? Build own OIDC layer or use managed? |
| **Adapter model** | Adapters as plugins, separate services, or in-process modules? How to version and deploy independently? |

**Deliverable:** Architecture Decision Records (ADRs) for each area.

### Phase 1 — Core Data Layer (short-term)
*Build the Economy Domain as a standalone service with production-grade data model.*

| Step | What | Depends on |
|---|---|---|
| 1.1 | **Economy Domain service** — canonical data model (facts, entities, relations, attributes) on real DB. Port proven POC patterns: adapter, staging pipeline, structural policies. | Phase 0 DB decision |
| 1.2 | **Fact pipeline** — received → validated → published with error policy, watermark, auto-revalidate. Port directly from POC. | 1.1 |
| 1.3 | **Adapter interface** — define the contract (capabilities, sync, member attributes). Build first production ERP adapter. | 1.1 |
| 1.4 | **Dimension routing** — configurable field-to-slot mapping. Port from POC `dim_routing` + code mappings. | 1.1 |
| 1.5 | **API layer** — REST/gRPC endpoints for Economy Domain CRUD. OpenAPI spec. | 1.1 |
| 1.6 | **Multi-tenant data isolation** — tenant context in all queries. | Phase 0 tenancy decision |

**Validates:** Can we run multiple products against a shared Economy Domain with real data?

### Phase 2 — Integration Layer (short-term)
*Event routing and cross-product data flow.*

| Step | What | Depends on |
|---|---|---|
| 2.1 | **Event broker setup** — topics, consumer groups, DLQ, retry policy. Schema registry with Avro/Protobuf. | Phase 0 broker decision |
| 2.2 | **Ingress → Enrichment → Egress** — port POC router pattern: canonical ID injection, dim routing, planning dimension mapping. | 1.4, 2.1 |
| 2.3 | **Dimension snapshot publishing** — publish to products on change (content-hash diff). | 1.1, 2.1 |
| 2.4 | **Identity mapping service** — cross-system entity linking. | 2.1 |
| 2.5 | **Product ingestion SDK** — client library for products to consume platform events (dimension snapshots, facts, notifications). | 2.1 |

**Validates:** Can two existing Hypergene products receive enriched data through the platform?

### Phase 3 — Identity & Portal (mid-term)
*User-facing platform with authentication and administration.*

| Step | What | Depends on |
|---|---|---|
| 3.1 | **OIDC/SAML2 integration** — real IdP redirect flow, token exchange, session management. | Phase 0 auth decision |
| 3.2 | **SCIM provisioning** — production SCIM 2.0 endpoint for user lifecycle. | 3.1 |
| 3.3 | **Platform Admin portal** — proper SPA for configuration, Economy Domain management, audit trail. Port proven admin UI patterns. | Phase 0 frontend decision |
| 3.4 | **Shell.js v2** — production-grade shell injection with CSP compliance, versioning, CDN delivery. | ✅ POC-proven (v2 implemented: error isolation, bootstrap, CSP, configurable URL) |
| 3.5 | **Help service** — shared knowledge base with per-tenant content. | 3.3 |
| 3.6 | **RBAC** — role → permission mapping, dimension-scoped access. | 3.1, 1.1 |

**Validates:** Can we onboard a customer with their IdP and have users navigate across products seamlessly?

### Phase 4 — Advanced Capabilities (long-term)
*Extend the platform with higher-value features.*

| Step | What | Depends on |
|---|---|---|
| 4.1 | **Multiple source adapters** — support 2+ ERP systems simultaneously, conflict resolution. | 1.3 |
| 4.2 | **Historical dimension versions** — point-in-time queries, period locking, version comparison. | 1.1 |
| 4.3 | **Process management** — budget version lifecycle (open → assign → collect → close), cross-product workflow orchestration. | 2.2, 3.6 |
| 4.4 | **Fine-grained authorization** — per-entity, per-org-unit, per-dimension permissions with policy engine. | 3.6 |
| 4.5 | **AI services** — shared RAG pipeline, per-product tool calling, agentic capabilities. | 3.5 |
| 4.6 | **External calculation** — import/export of budget data with `planning_source` partitioning (Excel, API, custom). | 2.2 |
| 4.7 | **Micro-frontend architecture** — shell as module federation host, products as independent remotes. | 3.4 |

---

## 3. Key Constraints to Investigate Per Product

Each existing product brings its own technical reality. Before integrating, investigate:

| Area | Questions |
|---|---|
| **Current data model** | How does the product store dimensions, hierarchies, facts today? What migration path to shared Economy Domain? |
| **Authentication** | Current auth mechanism? Can it delegate to platform OIDC? Cookie domain requirements? |
| **Deployment model** | SaaS, on-prem, hybrid? Impacts multi-tenancy approach and event broker topology. |
| **Frontend technology** | React, Angular, jQuery, server-rendered? Determines shell injection feasibility vs micro-frontend need. |
| **API surface** | Does the product expose APIs that the platform can call? Or only database-level integration? |
| **Dimension model** | Named dimensions vs generic slots? Fixed schema vs flexible? Mapping complexity. |
| **Change velocity** | How often does the product release? Can it adopt platform SDKs independently? |

---

## 4. What the POC Has Proven

| Concept | Verdict | Confidence |
|---|---|---|
| Shared Economy Domain with adapter pattern | Works well — clean separation of source-specific and canonical data | High |
| Event-driven routing with enrichment | Works — canonical IDs, dim routing, planning dimensions all function correctly | High |
| Structural policies (auto_root, grouping, attribute-based) | Works — flexible, configurable, composable | High |
| Shell injection for cross-product UX | Works — v2 adds error isolation, bootstrap endpoint, CSP compliance, configurable URL, versioning | High |
| Fact pipeline with quarantine / auto-revalidate | Works — industry-standard pattern, proven reliable | High |
| Dynamic adapter discovery (capabilities + flex-dims) | Works — source declares → platform discovers → routes automatically | High |
| Admin UI for platform configuration | Works for POC — needs real frontend framework for production | Medium |
| SCIM/OIDC simulation | Pattern is right — needs real IdP integration testing | Low–Medium |

---

## 5. Open Questions

- [ ] Which existing product is the best candidate for first integration (lowest friction, highest value)?
- [ ] What is the acceptable latency for event routing (real-time vs near-real-time vs batch)?
- [ ] How do existing products handle dimension updates today — full reload or incremental?
- [ ] Is there an existing customer willing to pilot the platform layer alongside current products?
- [ ] What is the team structure — dedicated platform team, or distributed ownership?
- [ ] Governance model — who owns the canonical dimension model? Platform team, customer, or product teams?

---

## 6. Product Integration Requirements

*Any product that participates in the platform ecosystem must meet a set of baseline requirements. These are not optional — they are the contract between the platform and each product.*

### 6.1 Economy Domain Compliance

| # | Requirement | Description |
|---|---|---|
| P1 | **Accept canonical dimensions** | Product must consume dimension snapshots from the platform (`DimensionSnapshot` events) and store entities + relations locally. Product must NOT maintain its own master dimension list independently of the platform. |
| P2 | **Use dim-slot model** | Product must support the platform's dimension model: named core dimensions (account, org_unit, project) + generic slots (dim1–dimN). Product may assign local labels (e.g. dim1 = "Activity") but must preserve slot identity in data exchange. |
| P3 | **Accept enriched facts** | Product must consume fact events (GL, budget) with platform-enriched dimension values (`dim_values_per_entry` / `dim_values_per_line`). Product must not re-derive dimension mappings that the platform already provides. |
| P4 | **Publish facts in canonical format** | When a product produces transactional data (e.g. budget lines), it must publish them in the platform's canonical fact format: source_system, period, amount, account, org_unit, dim1–dimN. |
| P5 | **Support structural policies** | Product must accept and render platform-generated structural nodes (auto_root `_ALL`, auto_missing `_MISSING`, grouping nodes `_GRP_*`). Product may hide system nodes in end-user UIs but must preserve them in data. |
| P6 | **Attribute awareness** | Product must accept entity attributes from dimension snapshots. Product may extend with local attributes but must not overwrite platform-sourced attributes. |

### 6.2 Integration & Events

| # | Requirement | Description |
|---|---|---|
| P7 | **Event-driven communication** | Product must communicate with the platform exclusively through events (Kafka/message broker). No direct database access between products or from product to platform DB. |
| P8 | **Idempotent event consumers** | Product must handle re-delivery of events gracefully (at-least-once semantics). Use event_id or content-based deduplication. |
| P9 | **Publish domain events** | Product must publish meaningful domain events (e.g. BudgetSubmitted, TaskAssigned) to its designated ingress topic. The platform enriches and routes them. |
| P10 | **Accept canonical IDs** | Product must store and use the platform's canonical entity IDs (or cross-reference mapping) when referring to shared entities. Product-local IDs are fine internally but must not leak to other systems. |
| P11 | **Expose capabilities API** | Product should expose a `/api/capabilities` endpoint (or equivalent metadata) describing its data model, supported dimensions, and event types — enabling the platform to auto-discover and configure routing. |

### 6.3 Identity & Access

| # | Requirement | Description |
|---|---|---|
| P12 | **Delegate authentication** | Product must not maintain its own user database or login flow. Authentication is delegated to the platform (OIDC/SAML2). Product receives a JWT and trusts the platform's token validation. |
| P13 | **Respect product entitlements** | Product must check the user's `products` claim in the JWT and reject access if the user is not entitled to this product. |
| P14 | **Accept RBAC context** | Product must read role and org_unit (and future permission claims) from the JWT and apply appropriate access controls. Product may define product-specific permissions but must respect platform-level roles. |
| P15 | **Support shell injection** | Product must include the platform's shell script (`shell.js`) in all user-facing pages. The shell provides navigation, inbox, help, and cross-product UX. Product must not render its own top-level navigation that conflicts with the shell. |

### 6.4 User Experience

| # | Requirement | Description |
|---|---|---|
| P16 | **Shell-compatible layout** | Product UI must reserve space for the shell header (CSS variable `--shell-height`). Product's own navigation bar (if any) must sit below the shell, never above or overlapping. |
| P17 | **Deep-link support** | Product must support URL-based navigation so the shell, inbox items, and help articles can link directly to specific views or entities within the product. |
| P18 | **Toast notification API** | Product should use the shell's toast notification mechanism (`window.shellToast()`) for cross-product notifications rather than implementing its own. |
| P19 | **Help integration** | Product should register context-sensitive help links using the shell's deep-link API (`window.shellOpenHelp('slug')`) so users can access relevant help articles from within product UIs. |
| P20 | **Consistent design language** | Product should follow the platform's design tokens (primary color, typography, spacing) for shared UI elements. Product may have its own accent color for product-specific branding. |

### 6.5 Technical Baseline

| # | Requirement | Description |
|---|---|---|
| P21 | **API-first** | Product must expose a documented API (REST or gRPC) for all data it shares with the platform or other products. No screen-scraping, file drops, or direct DB queries. |
| P22 | **Health endpoint** | Product must expose a `/health` endpoint returning service status. The platform monitors product availability. |
| P23 | **Containerized deployment** | Product must be deployable as a container (Docker/OCI). The platform orchestrates products as part of a composed environment. |
| P24 | **Configuration via environment** | Product must accept platform-provided configuration (broker URL, platform API URL, tenant ID) via environment variables — not hardcoded. |
| P25 | **Graceful degradation** | Product must continue to function (in reduced capacity) if the platform event broker is temporarily unavailable. Sync resumes automatically when connectivity is restored. |

### 6.6 Integration Maturity Levels

Products may adopt platform integration incrementally. Not everything is required on day one.

| Level | Name | What's Required | Value |
|---|---|---|---|
| **L0** | Standalone | Product works independently, no platform integration. | Baseline — no shared capabilities. |
| **L1** | Shell & Auth | P12–P16, P22. Product delegates auth and renders the shell. Users get unified navigation and SSO. | Quick win — unified login and navigation across products. |
| **L2** | Dimension Consumer | L1 + P1–P2, P5–P6. Product receives dimensions from the platform instead of maintaining its own. | Shared master data — one source of truth for dimensions. |
| **L3** | Fact Consumer | L2 + P3, P7–P8, P10. Product receives enriched facts (GL, budget) through platform events. | Cross-product data flow — actuals and budget in one place. |
| **L4** | Fact Producer | L3 + P4, P9. Product publishes its own data back through the platform for other products to consume. | Bi-directional data sharing — full platform participation. |
| **L5** | Full Integration | L4 + P11, P17–P21, P24–P25. Capabilities API, deep-linking, API-first, graceful degradation. | Production-grade platform citizen. |
