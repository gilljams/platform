# Arkitekturell genomlysning — Platform POC

> Granskad: 2026-04-29  
> Kontext: Multi-produkt roll-up-strategi, löst kopplade produkter via plattformslager

## Sammanfattning

POC:n visar en **mogen och modern arkitekturförståelse**. Event-driven, löst kopplad, plattformslager som medlare — det är rätt mönster för en multi-produkt roll-up-strategi. Nedan bryts det ner i styrkor, svagheter och förbättringsförslag.

---

## ✅ Styrkor (vad som är rätt och modernt)

### 1. Event-Driven Architecture (EDA) med Kafka/Redpanda
Exakt så ledande plattformsföretag (Salesforce, Atlassian, Visma, Hubspot) separerar produkter. Redpanda ger Kafka-semantik utan ZooKeeper-overhead. Ingress/Egress-topicmodellen med tydliga namnrymder (`erp.projects`, `platform.projects.out`) är best practice.

### 2. Canonical ID — identitetsmediering
Platform äger identiteten mellan system (`getOrCreateCanonical`). Detta är kärnan i varje roll-up-strategi: produkter behöver inte veta om varandra, bara plattformen gör mappningen. Jämförbart med hur Visma Connect hanterar kund-ID tvärs över Vismas 200+ produkter.

### 3. Typed Event Contracts (`shared/events.ts`)
Compile-time kontrakt via TypeScript unions (`AccountsPublished | ProjectCreated | ...`). Alla producenter och konsumenter delar definitioner utan runtime-beroende. Pragmatiskt val för POC-nivå.

### 4. Economy Domain + System Config
Economy Domain (econ_*-tabeller) är ett standardiserat staginglager för all ekonomisk referensdata. `system_config` (nyckel-värde per system) ersätter den tidigare Connector Registry:n. Plattformen är **ERP-oberoende** — nya datakällor behöver bara en adapter som skriver till econ_*-tabellerna. System registreras med `system_type` (erp/budgeting/analytics) och `task_base_url` för deep links.

### 5. Shell.js — Microfrontend-liknande injection
Cross-product navigation, SSO-kontroll, inbox-badge — allt injicerat via ett script. Liknar Atlassians "Connect" iframe-modell men enklare. Produkterna behöver inte veta om varandra.

### 6. Dimension Routing Engine
`applyDimRouting()` är en generisk regelmotor som översätter källsystemets fält till målets flex-dimensioner. Jämförbart med ETL-mapping i Workday/Anaplan — men som realtids-event-enrichment.

### 7. Docker Compose med health checks
Alla tjänster har `healthcheck` + `depends_on: service_healthy`. Bra signal om produktionsmognad i tänket.

---

## ⚠️ Svagheter & risker

### 1. SQLite i varje container
Bra för POC — men produktionsomöjligt för multi-instans/HA. Ingen replikering, risk för WAL-lock vid concurrency.  
→ *Produktionsväg:* PostgreSQL per tjänst, eller managed DB-tjänst (RDS, CloudSQL).

### 2. Inget Schema Registry
`shared/events.ts` ger compile-time kontrakt men ingen runtime-validering. Om Product A publicerar ett event med fel format kraschar konsumenten (inte producenten).  
→ *Förbättring:* Confluent Schema Registry (Avro/Protobuf) eller JSON Schema-validering vid ingress.

### 3. ~~In-memory Event Log (ring buffer 200)~~
~~`router.ts` hade `eventLog[]` med max 200 poster — inga persistenta audit trails. Vid restart försvinner allt.~~  
→ ✅ **Implementerat:** Persistent `audit_events`-tabell i SQLite skrivs parallellt med in-memory log. Se implementation nedan.

### 4. ~~Auth/Security — minimalmodell~~
~~JWT i cookie, `parseJwt` utan signaturverifiering i shell.js, inga middleware guards på API-endpoints, SCIM-token är hårdkodad.~~  
→ ✅ **Implementerat:** Central `requireAuth`-middleware som verifierar JWT-signatur på alla `/api/*` endpoints. Publika paths (login, logout, SCIM, health) undantagna. `req.user` tillgänglig i alla handlers.  
→ *Kvarstår för produktion:* OIDC provider (Keycloak/Auth0), RS256 + JWKS, RBAC per endpoint.

### 5. Monolitisk HTML med inline JS
`product-a/public/index.html` och `admin.html` är stora single-file HTML-sidor med inline `<script>`. Svårt att testa, no bundling, no component reuse.  
→ *Produktionsväg:* React/Vue/Svelte per produkt, micro-frontend orchestration (Module Federation eller importmaps).

### 6. ~~Ingen idempotens-hantering~~
~~Kafka consumer med `fromBeginning: true` — om en tjänst startar om processas alla events igen utan dedup.~~  
→ ✅ **Implementerat:** `processed_events`-tabell per tjänst med event_id-baserad dedup. Se implementation nedan.

### 7. ~~Saknar API-versionering~~
~~Alla endpoints är `/api/...` utan version. Breaking changes kräver big bang-deployment.~~
→ ✅ **Implementerat:** URL-rewrite middleware som accepterar `/api/v1/` parallellt med `/api/`. Se implementation nedan.

---

## 🏢 Jämförelse med ledande plattformar

| Aspekt | POC | Salesforce | Atlassian | Visma | Hubspot |
|---|---|---|---|---|---|
| **Event bus** | Redpanda (Kafka) | Platform Events (Kafka) | Atlassian EventBridge | Azure Service Bus | Internal Kafka |
| **Identity mediation** | Canonical ID | Salesforce ID | Atlassian Account | Visma Connect ID | Hubspot Object ID |
| **API discovery** | system_config + econ_* | Metadata API | Forge manifest | RAET API registry | API catalog |
| **Shell/Chrome** | shell.js injection | Lightning Web | Atlassian Navigation | Visma Frame | HubSpot shell |
| **Auth** | JWT cookie | OAuth 2.0 + OIDC | Atlassian Connect JWT | Visma Connect OIDC | OAuth 2.0 |
| **Data coupling** | Shared dimensions | Shared objects | Cross-product GraphQL | Master Data Hub | Shared schemas |

**Observation:** POC:n följer samma mönster som dessa plattformar på konceptuell nivå. Det som skiljer är mognadsgrad i auth, API governance, schema management och observability.

---

## 📊 Bedömning per princip

| Princip | Betyg | Kommentar |
|---|---|---|
| **Loose coupling** | ⭐⭐⭐⭐⭐ | Produkter kommunicerar enbart via events + REST. Inget direkt beroende. |
| **Event-driven** | ⭐⭐⭐⭐⭐ | Korrekt EDA med ingress/egress, routing, enrichment. |
| **Single responsibility** | ⭐⭐⭐⭐ | Platform gör routing+auth+admin — borde separeras i produktion. |
| **Schema governance** | ⭐⭐ | TypeScript-kontrakt utan runtime-validering eller versionering. |
| **Security** | ⭐⭐⭐⭐ | ✅ Central JWT-verifiering på alla API-routes. OIDC/RBAC kvarstår för prod. |
| **Observability** | ⭐⭐⭐ | Jaeger + OTEL i compose, men ingen tracing implementerad i koden. |
| **Testbarhet** | ⭐⭐ | Inga automatiserade tester, monolitisk HTML. |
| **Twelve-Factor** | ⭐⭐⭐⭐ | Config via env, port-per-tjänst, stateless processes (utom SQLite). |
| **Idempotens** | ⭐⭐⭐⭐ | ✅ Implementerad dedup via `processed_events`-tabell. |
| **Audit trail** | ⭐⭐⭐⭐ | ✅ Persistent event log i DB parallellt med in-memory ring buffer. |
| **API governance** | ⭐⭐⭐⭐ | ✅ Versioned endpoints (`/api/v1/`) med bakåtkompatibilitet. |
| **DevX** | ⭐⭐⭐⭐⭐ | `docker compose up`, demo-runner med 11 steg, reset-endpoint. |

---

## 🎯 Rekommendationer (rankat efter impact)

| # | Åtgärd | Status | Impact |
|---|---|---|---|
| 1 | Schema Registry / JSON Schema vid ingress | 📋 Framtida | Hög — förhindrar korrupta events |
| 2 | Idempotent consumers (dedup per event_id) | ✅ Implementerad | Hög — säker restart |
| 3 | Persistent event log / audit trail | ✅ Implementerad | Medel — full spårbarhet |
| 4 | Auth middleware (centralt JWT-lager) | ✅ Implementerad | Hög — security baseline |
| 5 | API versioning (`/api/v1/`) | ✅ Implementerad | Medel — billigt tidigt |
| 6 | Component-based frontend | 📋 Framtida | Medel — testbarhet & reuse |

---

## 🔧 Implementerade förbättringar

### Persistent Event Log (audit_events)

**Fil:** `platform/src/mapper.ts` + `platform/src/router.ts`

Ny tabell `audit_events` som skrivs vid varje event som passerar routern. Bevarar full historik oavsett restart. In-memory ring buffer (200 poster) behålls för snabb API-access.

```sql
CREATE TABLE IF NOT EXISTS audit_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  direction TEXT NOT NULL,        -- 'in' | 'out'
  topic TEXT NOT NULL,
  event_type TEXT NOT NULL,
  canonical_id TEXT,
  summary TEXT,
  timestamp TEXT NOT NULL
);
```

### Idempotent Consumers (processed_events)

**Filer:** `platform/src/mapper.ts` + `platform/src/router.ts`, `product-a/src/index.ts`, `product-b/src/index.ts`

Varje tjänst har en `processed_events`-tabell. Innan ett event processas kontrolleras `event_id` — om det redan finns hoppas det över. Säkerställer at-least-once → effectively-once semantik.

```sql
CREATE TABLE IF NOT EXISTS processed_events (
  event_id TEXT PRIMARY KEY,
  processed_at TEXT NOT NULL
);
```

### API Versioning (URL rewrite middleware)

**Filer:** `platform/src/index.ts`, `product-a/src/index.ts`, `product-b/src/index.ts`

Middleware som transparently rewrites `/api/v1/*` → `/api/*`. Alla endpoints svarar på båda prefix.
När v2 behövs registreras separata routes — v1 fortsätter fungera oförändrat.

```typescript
app.use((req, _res, next) => {
  if (req.path.startsWith("/api/v1/")) {
    req.url = req.url.replace("/api/v1/", "/api/");
  }
  next();
});
```

### Auth Middleware (requireAuth)

**Fil:** `platform/src/index.ts`

Central middleware som verifierar JWT-signatur på alla `/api/*` routes automatiskt. Publika endpoints undantas explicit.

```typescript
// Publika paths (inget auth-krav)
const PUBLIC_PATHS = ["/api/login", "/api/logout", "/api/scim/v2", "/health"];

app.use((req, res, next) => {
  if (!req.path.startsWith("/api/")) return next();
  if (PUBLIC_PATHS.some(p => req.path === p || req.path.startsWith(p + "/"))) return next();
  // Verify JWT signature
  const token = req.cookies?.platform_token;
  if (!token) return res.status(401).json({ error: "Authentication required" });
  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    res.status(401).json({ error: "Invalid or expired token" });
  }
});
```

Effekt: Alla nya endpoints som läggs till under `/api/` skyddas automatiskt utan extra kod.

---

## Slutsats

**POC:n demonstrerar ett arkitekturmönster som ligger i framkant** för multi-produkt-plattformar. Det event-drivna mönstret med plattformen som identitetsmedlare och routing-lager är precis hur Visma, Atlassian och Salesforce bygger sina plattformar. Svagheterna (SQLite, avsaknad av schema registry, minimal auth) är helt förväntade på POC-nivå och visar tydligt *var* investeringar behövs för att gå till produktion. Arkitekturen är **sund och skalbar i sin grunddesign**.

De implementerade förbättringarna (persistent audit log, idempotent consumers, API-versionering, auth middleware) fungerar som **referensimplementationer** som visar exakt hur dessa mönster realiseras i kod — värdefullt som blueprint vid framtida produktionsutveckling.
