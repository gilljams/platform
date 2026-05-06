# Platform POC — Projektplan

## Mål
Demonstrera en event-driven arkitektur med:
- Två oberoende produkter (Product A och Product B)
- En mock-ERP som extern källa
- Ett plattformslager som hanterar event-ingestion, identity mapping och event routing
- Observability via OpenTelemetry + Jaeger

## Beslut

| Fråga | Beslut |
|---|---|
| Språk | Node.js + TypeScript |
| Event broker | Redpanda (Kafka-kompatibel, en container) |
| Databas | SQLite per tjänst (better-sqlite3) |
| Tracing | OpenTelemetry + Jaeger |
| Länkning fictive→real | Manuell via Platform API |
| UI Product A | Multi-view HTML: Översikt (projekt + resiliens) + Budgetinmatning (spreadsheet-grid) |
| UI Product B | Enkel HTML: grid med ERP-utfall + budget från Product A |
| Periodformat | År-månad (`2025-01`) — inte kvartalsformat |
| Budget-versioner | Utkast→Inskickad modell: spara lokalt, explicit submit publicerar till Kafka |
| Planning-dimensioner | Platform översätter Product A:s versionsnamn till Product B:s dimensioner |
| Grafana | Nej, Jaeger räcker |
| Flex-dimensioner | Approach C: namngivna kärndims + generiska dim1-dim3 slots |
| Dimension Mapping | Shared Taxonomy: plattformen äger kanonisk kodlista, per-produkt mappning |
| Admin UI-struktur | 8 flikar: Configuration → Master Data → Domains → Identity & Access → Events → Demo → Help → POC & Production |
| Budget dim-routing | Budget berikas med flex-dims via samma applyDimRouting-mekanism som GL |
| Economy Domain | Standardiserat staginglager (econ_*) — adapters transformerar källdata till gemensamt format. Ersätter Connector Registry. |

## ID-standard

Varje system har ett tydligt prefix. Plattformen äger det canonical ID som binder samman alla.

| System | Prefix | Exempel | Ägs av |
|---|---|---|---|
| **Platform** | `platform-` | `platform-001` | Plattformen |
| **ERP** | `erp-` | `erp-042` | ERP |
| **Product A** | `prod_a-` | `prod_a-001` | Product A |
| **Product B** | `prod_b-` | `prod_b-001` | Product B |

Mappningsexempel i Platform:
```
canonical_id: "platform-001"
  → erp_id:      "erp-042"
  → prod_a_id:   "prod_a-001"
  → prod_b_id:   "prod_b-001"
```

Produkterna skickar sina egna ID:n i events. Plattformen berikar med `canonical_id` vid routing.
Ingen produkt behöver känna till en annan produkts ID — plattformen är den enda länken.

## Miljö (verifierad)

- **Node.js** v24.13.1 ✅
- **npm** 11.8.0 ✅
- **Git** 2.52.0 ✅
- **Docker Desktop** v29.4.0 ✅
- **Docker Compose** v5.1.2 ✅
- **WSL** uppdaterad ✅

## Arkitektur

```
 ┌──────────┐   SCIM 2.0    ┌──────────────────────────────────────────────────────┐
 │   IdP    │──provision───►│              PLATFORM LAYER (:3000)                  │
 │(simulerad│               │                                                      │
 │ i demo)  │               │  ┌────────────┐  ┌───────────┐  ┌────────────────┐  │
 └──────────┘               │  │  Identity   │  │  Economy   │  │  Event Router  │  │
                            │  │  & Access   │  │  Domain    │  │  & Enrichment  │  │
                            │  │             │  │ (econ_*)   │  │                │  │
                            │  │ users       │  │ entities   │  │ dim routing    │  │
                            │  │ SCIM/OIDC   │  │ relations  │  │ canonical IDs  │  │
                            │  │ JWT tokens  │  │ facts      │  │ planning dims  │  │
                            │  │ groups      │  │ attributes │  │ code mapping   │  │
                            │  └────────────┘  │ sync_state │  └───────┬────────┘  │
                            │                  │            │          │            │
                            │  ┌────────────┐  │ ┌────────┐│  ┌───────▼────────┐  │
                            │  │  Shell.js   │  │ │Adapter ││  │ Kafka Consumer │  │
                            │  │ (injected)  │  │ │Pattern ││  │ ingress→enrich │  │
                            │  │ nav, inbox, │  │ │ERP→econ││  │ →egress topics │  │
                            │  │ ext tools   │  │ └────────┘│  └────────────────┘  │
                            │  └────────────┘  └───────────┘                      │
                            └──────────────────────────┬───────────────────────────┘
                                                       │
                                          GET /shell.js │ Egress events
                              ┌────────────────────────┼────────────────────┐
                              │                        │                    │
                              ▼                        ▼                    ▼
                  ┌──────────────────┐   ┌──────────────────┐  ┌──────────────────┐
                  │    ERP Mock      │   │   Product A      │  │   Product B      │
                  │   (:3001)        │   │  (:3002)         │  │  (:3003)         │
                  │                  │   │  Budget &        │  │  Analytics       │
                  │  Accounts        │   │  Planning        │  │                  │
                  │  Projects        │   │                  │  │  Ingestion       │
                  │  General Ledger  │   │  BudgetSubmitted │  │  Pipeline        │
                  │  (källdata)      │   │  TaskAssigned    │  │  (default/derive │
                  └────────┬─────────┘   └────────┬─────────┘  │   regler)        │
                           │                      │            └──────────────────┘
                           │                      │                    ▲
                           ▼                      ▼                    │
                  ┌────────────────────────────────────────────────────────────────┐
                  │                      REDPANDA (Kafka)                          │
                  │                                                                │
                  │  Ingress:  erp.accounts  erp.projects  erp.general-ledger     │
                  │            product-a.events  product-a.tasks                   │
                  │                                                                │
                  │  Egress:   platform.accounts.out  platform.projects.out        │
                  │            platform.budget.out    platform.gl.out              │
                  │            platform.entity-linked.out  platform.dimensions.out │
                  └────────────────────────────────────────────────────────────────┘
```

### Dataflöde — Economy Domain

```
Källa (ERP)                      Platform Economy Domain                  Produkter
───────────                      ───────────────────────                  ─────────
                    Adapter
AccountsPublished ──(runEconSync)──► econ_entities
                                    econ_relations (hierarkier)
                                    econ_attribute_defs
                                    econ_entity_attributes

                    Fact Pipeline
GeneralLedger     ──────────────────► econ_facts (staging_status)
Published                              received → validated → published
                                       ↓
                                    Kafka: platform.gl.out ──────────► Product B
                                    (berikad med dim_values_per_entry)   (ingestion pipeline)

                    Shared Dimensions API (delegerar till econ_*)
                    ┌────────────────────────────────────────┐
                    │ upsertDimensionCode → upsertEconEntity │
                    │ getDimensionCodes   → SELECT econ_*    │
                    │ setHierarchy        → upsertEconRelation│
                    └────────────────────────────────────────┘
```

> **POC vs Produktion:** I produktion är ERP-systemet och Economy Domain två helt separata system — ERP:et ägs av kunden och Economy Domain lever i plattformens backend. I denna POC nyttjar vi ERP Mock-miljön (`localhost:3001`) för att visa *båda perspektiven* sida vid sida: källdatan som den ser ut i ERP:et, och hur den transformeras till standardiserat Economy Domain-format via adaptern.

**Nyckelprinciper:**
- **Economy Domain** (`econ_*`) är plattformens standardiserade staginglager — ERP-oberoende
- **Adapter-mönster**: `runEconSync` transformerar källdata (ERP API) till Economy Domain-format
- **Fact Pipeline**: 3-stegs validering (received → validated → published) med idempotent sync (se nedan)
- **Dim Routing**: `applyDimRouting` översätter källfält till dim-slots via `dim_routing` + `dimension_code_mappings`
- **Shell injection**: Alla produkter inkluderar `/shell.js` som renderar gemensam header med nav, inbox och externa verktyg
- **SCIM 2.0**: Användare provisioneras från IdP; `parseGroupClaims` mappar gruppnamn till roll/produkt/org

### Fact Pipeline — Idempotent GL-ingestion

GL-transaktioner flödar ERP → Platform → Produkter genom en **trelagers-idempotent pipeline**.
Designen förhindrar dubbletter vid omsync, stödjer periodbaserad omläsning och använder high watermark för inkrementell sync.

**Trelagersmodell:**

| Lager | Mekanism | Dedup-strategi |
|-------|----------|----------------|
| **ERP (källa)** | Pull-baserat API: `GET /api/gl` med `period_from`, `period_to`, `modified_since` | Varje rad har ett deterministiskt `entry_id` (t.ex. `gl-3010-OU-100-2025-01`) och `modified_at` |
| **Platform (staging)** | `econ_facts` med `UNIQUE(source_system, source_row_id)`, `ON CONFLICT DO UPDATE` (upsert) | Samma rad från samma källa skriver alltid över — aldrig dubbletter. Periodomsync raderar gamla fakta först |
| **Produkt (konsument)** | Läser `sync_mode` från Kafka-event. Vid `replace_by_period`: raderar `gl_lines` för berörda perioder före insert | Periodnivå-atomicitet: gammal data tas bort, ny sätts in |

**Sync-lägen:**

| Läge | Beteende |
|------|----------|
| **Inkrementell** (default) | Använder `high_watermark` (senaste `modified_at`). Hämtar bara rader modifierade efter watermark. Snabbt, minimal datatransfer |
| **Periodomläsning** | Admin anger `period_from` / `period_to`. Ignorerar watermark, hämtar alla rader för perioden, raderar gamla stagingdata, och upsertar. Användbart vid retroaktiva korrigeringar i källsystemet |
| **Full sync** | Inget periodfilter, ingen watermark. Hämtar allt. Upsert säkerställer inga dubbletter. Säkert att köra som "reset" men långsammare |

**High Watermark:**
- Lagras i `sync_state.high_watermark` per källa + entity_type
- Visar senaste `modified_at` sedd från ERP
- Vid varje inkrementell sync hämtas bara rader nyare än watermärket
- Efter lyckad sync avanceras watermärket till nyaste timestamp i batchen
- Visas i Sync State-tabellen i Admin UI

**Kafka-event (platform.gl.out) utökat schema:**
```json
{
  "sync_mode": "replace_by_period",
  "periods": ["2025-01", "2025-02", "2025-03"],
  "entries": [...]
}
```
Produkten läser `sync_mode` och `periods` för att avgöra om gammal data ska raderas före insert.

## Komponenter (7 containers)

| Container | Teknologi | Port |
|---|---|---|
| `redpanda` | Redpanda | 19092 (Kafka), 8081 (Schema Reg), 8082 (Admin) |
| `redpanda-console` | Redpanda Console | 8080 (UI) |
| `jaeger` | Jaeger all-in-one | 16686 (UI), 4318 (OTLP) |
| `erp-mock` | Node.js/Express | 3001 (+ Economy Domain Explorer UI) |
| `product-a` | Node.js/Express + SQLite | 3002 |
| `product-b` | Node.js/Express + SQLite | 3003 |
| `platform` | Node.js/Express + SQLite | 3000 |

## Events

| Event | Källa | Beskrivning |
|---|---|---|
| `AccountsPublished` | ERP Mock | Kontoplan + org-enheter (referensdata) |
| `ProjectCreated` | ERP Mock | Riktigt projekt skapat i ERP |
| `GeneralLedgerPublished` | ERP Mock | Utfall från huvudbok |
| `BudgetProjectCreated` | Product A | Fiktivt budgetprojekt skapat |
| `BudgetUpdated` | Product A | Budgetpost tillagd/ändrad (legacy — ej i nuvarande demoflöde) |
| `BudgetSubmitted` | Product A | Budget skickad in (utkast→inskickad), publiceras efter explicit submit |
| `TaskAssigned` | Product A | Uppgift tilldelad en användare (budgetversion öppnad/tilldelning tillagd) |
| `TaskCompleted` | Product A | Uppgift markerad som klar (assignment complete → inbox done) |
| `ProjectLinked` | Platform | Fiktivt projekt länkat till riktigt |

### Event-basschema

Alla events innehåller:
```typescript
{
  event_id: string;          // UUID
  event_type: string;        // T.ex. "ProjectCreated"
  timestamp: string;         // ISO 8601
  source_system: string;     // "erp" | "prod_a" | "prod_b" | "platform"
}
```

### GeneralLedgerEntry-schema

```typescript
{
  entry_id: string;          // Deterministiskt: "gl-{account}-{org_unit}-{period}" — stabil ID för idempotent upsert
  erp_id: string;            // T.ex. "erp-042"
  org_unit: string;          // T.ex. "OU-100"
  account: string;           // T.ex. "4010"
  period: string;            // T.ex. "2025-01" (år-månad)
  amount: number;
  currency: string;          // T.ex. "SEK"
  modified_at: string;       // ISO 8601 — används för high watermark-tracking
  activity?: string;         // T.ex. "AKT-100" — flex-dim (routas till dim1)
  cost_bearer?: string;      // T.ex. "KB-500" — flex-dim (routas till dim2)
  counterpart?: string;      // T.ex. "MP-200" — flex-dim (routas till dim3)
}
```

## Kafka Topics

### Ingress-topics (producenter → Platform)
Dessa topics skrivs av källsystemen. **Bara Platform konsumerar** dem.

| Topic | Innehåll | Producent | Konsument |
|---|---|---|---|
| `erp.projects` | ProjectCreated | ERP | Platform |
| `erp.accounts` | AccountsPublished | ERP | Platform |
| `erp.general-ledger` | GeneralLedgerPublished | ERP | Platform |
| `product-a.events` | BudgetProjectCreated, BudgetUpdated, BudgetSubmitted | Product A | Platform |
| `product-a.tasks` | TaskAssigned, TaskCompleted | Product A | Platform |

### Egress-topics (Platform → produkter)
Dessa topics skrivs **enbart av Platform** efter berikning med canonical_id.
Product A och B konsumerar **bara** dessa — aldrig ingress-topics.

| Topic | Innehåll | Konsumenter |
|---|---|---|
| `platform.accounts.out` | Kontoplan + org-enheter (vidarebefordrad) | Product A, Product B |
| `platform.projects.out` | Projekt (berikad med canonical_id) | Product A, Product B |
| `platform.budget.out` | Budget (berikad med canonical_id + planning_dimensions + dim_values_per_line) | Product B |
| `platform.gl.out` | Utfall (berikad med canonical_id + dim_values_per_entry) | Product B |
| `platform.entity-linked.out` | EntityLinked (identiteter sammankopplade) | Product B |
| `platform.dimensions.out` | DimensionSnapshot (entiteter + relationer per dimension) | Product A, Product B |

### Routingprincip
```
Källsystem  →  ingress-topic  →  Platform konsumerar
                                      ↓ berikar med canonical_id
                                      ↓ (BudgetSubmitted: + planning_dimensions)
                                 egress-topic  →  Produkter konsumerar
```
Produkterna känner aldrig till varandras topics. Plattformen är enda länken.

### Enrichment vid routing

**Budget-routing:** När Platform routar `BudgetSubmitted` till `platform.budget.out` berikas eventet med:
- `canonical_id` — för ID-mappning (som alla events)
- `planning_dimensions` — semantisk översättning av Product A:s "Budget 2025":
  ```json
  { "planning_year": "2025", "planning_type": "Budget", "planning_version": 1 }
  ```
- `dim_values_per_line` — flex-dim routing (källfält → dim-slots, samma mekanism som GL):
  ```json
  [{ "dim1": "AKT-100", "dim2": "KB-500", "dim3": "MP-200" }, ...]
  ```

**GL-routing:** När Platform routar `GeneralLedgerPublished` till `platform.gl.out` berikas eventet med:
- `canonical_id` — för ID-mappning
- `dim_values_per_entry` — flex-dim routing (ERP-fält → dim-slots):
  ```json
  { "dim1": "AKT-100", "dim2": "KB-500", "dim3": "MP-200" }
  ```

### Dimension Publishing Pipeline

Platform publicerar **DimensionSnapshot**-events till `platform.dimensions.out` efter varje synk och/eller policy-apply. En snapshot per dimension innehåller alla entiteter och relationer.

**Event-schema:**
```json
{
  "event_id": "uuid",
  "event_type": "DimensionSnapshot",
  "timestamp": "ISO 8601",
  "source_system": "economy_domain",
  "dimension": "account",
  "entities": [{ "code": "4010", "name": "Löner", "type": "leaf" }, ...],
  "relations": [
    { "child_code": "4010", "parent_code": "_ALL" },
    { "child_code": "4010", "parent_code": "40" },
    { "child_code": "40", "parent_code": "COSTS" },
    { "child_code": "COSTS", "parent_code": "RES" }
  ]
}
```

> **Hierarkimodell:** En snapshot kan innehålla **flera oberoende rötter**.
> `_ALL` är en flat lista av alla leaf-entiteter + `_MISSING` (system-nod).
> ERP-hierarkin (t.ex. RES → COSTS → 40 → 4010) lever som en separat rotstruktur.
> Grupp-noder (RES, COSTS, 30, 40 etc.) kopplas **inte** till `_ALL`.

**Structural Policies:**

Policies appliceras efter synk och skapar/underhåller systemgenererade strukturer.
Konfigureras per dimension (eller `*` för alla) via `POST /api/economy/policies`.

| Policy | Beteende |
|--------|----------|
| `auto_root` | Skapar `_ALL`-nod (type=system). Kopplar alla **leaf**-entiteter som barn till `_ALL`. Grupp-noder (type=group) lämnas orörda — de behåller sin ERP-hierarki. |
| `auto_missing` | Skapar `_MISSING`-nod (type=system) under `_ALL`. Används för transaktioner som refererar okända koder. |
| `grouping_rules` | Skapar grupp-noder baserat på regler (first_n_chars, char_range, regex) och kopplar leafs under matchande grupp. |

**Exekveringsordning:** auto_root → auto_missing → grouping_rules (inom varje dimension).

**Triggerpunkter:**
- `runEconSync` — efter adapter + structural policies har körts
- `POST /api/economy/policies/apply` — efter enskild dimension-apply
- `POST /api/economy/policies/apply-all` — efter alla dimensioner applicerats

**Mottagning i produkter:**
- Product A & Product B: prenumererar på `platform.dimensions.out`
- Ingestion: sparar entiteter i lokal `dim_members`-tabell + relationer i `dim_relations`-tabell
- Produkterna kan utöka med egen metadata (t.ex. `budgetable`, `aggregation_rule`)
- Admin Dimensions-flik: drill-down-vy med breadcrumb-navigation genom hierarkin

### Product B Ingestion Pipeline

Utöver plattformens routing har varje mottagande produkt sin egen **ingestion pipeline** — produktspecifika regler som appliceras per rad *efter* plattformens berikning men *innan* lagring.

**Ansvarsfördelning:**
- **Plattformen** ansvarar för routing *mellan system*: canonical IDs, dim-slot mapping, kodöversättning
- **Produkten** ansvarar för *intern* affärslogik: defaultvärden, härledda dimensioner, validering

**Regeltyper:**

| Typ | Beteende | Exempel |
|-----|----------|---------|
| `default` | Sätter värde **bara om fältet är tomt** | Om dim3 saknas → sätt till "STANDARD" |
| `derive` | **Skriver alltid över** baserat på villkor | Om account börjar med "3" → sätt dim2 = "REVENUE" |

**Regelschema (ingestion_rules):**
```sql
CREATE TABLE ingestion_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_name TEXT NOT NULL,
  source_type TEXT DEFAULT 'all',     -- 'budget', 'gl', 'all'
  rule_type TEXT NOT NULL,            -- 'default', 'derive'
  condition_field TEXT,               -- fält att kontrollera ('account', 'dim3', etc.)
  condition_op TEXT,                  -- 'IS NULL', '=', 'LIKE', 'IN'
  condition_value TEXT,               -- värde för villkor ('3%', 'OU-300')
  target_field TEXT NOT NULL,         -- fält att sätta ('dim2', 'dim3')
  target_value TEXT NOT NULL,         -- värde att sätta
  priority INTEGER DEFAULT 100,      -- lägre = körs först
  enabled INTEGER DEFAULT 1
);
```

**Flöde per rad:**
```
Kafka event in → Platform-berikad data
    ↓
For each line:
  1. Merge: original line + platform dim_values
  2. Load rules (WHERE enabled=1 AND source_type matches, ORDER BY priority)
  3. Per rule: if condition matches → set target_field
     - "default": only if target is NULL/empty
     - "derive": always overwrite
  4. Store enriched line
    ↓
Stored in budget_lines / gl_lines
```

**Seedade exempelregler:**

| Regel | Typ | Villkor | Resultat | Prioritet |
|-------|-----|---------|----------|-----------|
| Default dim3 | default | dim3 IS NULL | dim3 = "STANDARD" | 100 |
| Revenue accounts → dim2 | derive | account LIKE '3%' | dim2 = "REVENUE" | 50 |
| Personnel accounts → dim2 | derive | account LIKE '4%' | dim2 = "PERSONNEL" | 51 |
| External cost accounts → dim2 | derive | account LIKE '5%' | dim2 = "EXTERNAL" | 52 |
| Operating cost accounts → dim2 | derive | account LIKE '6%' | dim2 = "OPERATIONS" | 53 |

Regler hanteras via Product B:s UI (flik "Ingestion Rules") eller API:
- `GET /api/ingestion-rules` — lista alla regler
- `POST /api/ingestion-rules` — skapa ny regel
- `PUT /api/ingestion-rules/:id` — enable/disable
- `DELETE /api/ingestion-rules/:id` — ta bort regel

Detta konfigureras i grundinställningen (demosteg 1) och kan justeras manuellt via admin-UI.

## Platform Shell & Autentisering

### Login & Identity

Användare lagras i `users`-tabellen i Platform (SQLite). Demo-användare seedas automatiskt vid uppstart.
I produktion provisioneras användare via **SCIM 2.0** från en extern Identity Provider (IdP).

`POST /api/login` validerar mot `users`-tabellen. Returnerar en JWT som lagras som cookie (domän = localhost).
I produktion ersätts login med **OIDC/SAML2-redirect** till IdP (Zitadel, Azure AD, Okta).

**Arkitekturmönster (simulerat i POC):**
```
┌──────────┐   OIDC/SAML2    ┌──────────┐
│   IdP    │ ◄──redirect──── │ Platform │
│(Zitadel) │ ───id_token───► │          │
└──────────┘                 └──────────┘
      │                           │
      │ SCIM 2.0 sync            │ GET /api/users
      ▼                           ▼
 ┌──────────┐            ┌────────────┐
 │  users   │ ◄──read──  │ Product A/B│
 │ (SQLite) │            └────────────┘
 └──────────┘
```

**Vad som simuleras:**
- Login hanteras direkt (ingen redirect till IdP)
- SCIM-endpoint finns men triggas manuellt från admin-UI
- JWT-token speglar vad en OIDC-token skulle innehålla
- Admin-UI har "SCIM Provisioning"-simulator + Token & Session-vy

| Användare | Lösenord | Roll | Org-enhet | Produkter | Primär produkt |
|---|---|---|---|---|---|
| anna | demo | controller | OU-100 IT-avd | Product A, Product B | Product A |
| erik | demo | analyst | OU-200 Ekonomi | Product B | Product B |
| calle | demo | controller | OU-100 IT-avd | Product A | Product A |
| admin | demo | admin | HQ | Platform, Product A, Product B | Platform Admin |

- **anna** — har tillgång till båda produkterna → ser full shell header med navigering
- **erik** — bara Product B → minimal shell bar (4px remsa, expanderar vid hover)
- **calle** — bara Product A → minimal shell bar (4px remsa, expanderar vid hover)
- **admin** — plattformsadmin → landas på Platform Admin, ser alla tre i navigeringen

JWT payload:
```json
{
  "user_id": "user-001",
  "name": "Anna Svensson",
  "role": "controller",
  "org_unit": "OU-100",
  "products": ["product-a", "product-b"],
  "primary_product": "product-a"
}
```

### Platform Shell Header
Platform serverar `GET /shell.js` — ett litet script som alla vyer inkluderar:
```html
<script src="http://localhost:3000/shell.js"></script>
```

Scriptet renderar en **gemensam header** överst i varje produkt:

**Pinned (multi-produkt)** — full 32px bar med navigation, inbox och pin/unpin:
```
┌─────────────────────────────────────────────────────────────────────┐
│  Platform POC   Platform Admin │ Product A │ Product B │ Ext ↗    │   Inbox (3)  📌
└─────────────────────────────────────────────────────────────────────┘
```

**Unpinned** — bar dold, ersatt av en **notch pill** i övre högra hörnet:
```
                                                          ┌──────────────┐
                                                          │ Inbox (3)  📌│
                                                          └──────┬───────┘
                                                                 │ (dark tail)
```
- Notch-pill: vit U-formad content (`border-radius: 0 0 10px 10px`) + mörk bakgrund/tail (#0d1822)
- Notch döljs under peek (mouseenter) — `updateNotchVisibility()` kollar `header.classList.contains("peek")`
- Hovrar man över toppen visas full baren temporärt (peek-mode)

**Shell-funktioner:**
- Pin/unpin via knapp — `userHasToggledPin` flag förhindrar auto-toggle vid produktändringar
- Inbox-dropdown med aktiva tasks, badge döljs vid 0
- External tools: visas efter produktlänkar med ↗-ikon
- CSS-variabel `--shell-height` sätts så produkt-baren sticky-positionerar korrekt
- Navigeringslänkar visas **bara för produkter användaren har tillgång till**
- Entitlement-check: redirect om ej behörig
- Toast-notifikationer: vit bakgrund, färgad vänsterkant (grön/röd), SVG-ikoner
- Help panel: ? icon öppnar hjälppanel med artiklar, sök och deep-linking
- AI chat: ✦ icon öppnar AI-assistent med RAG-integration mot hjälpartiklar

### Help Service (Shared Platform Capability)

Plattformen tillhandahåller en **gemensam hjälptjänst** som alla produkter kan använda utan egen implementation.

**Arkitektur:**
```
┌─────────────────────────────────────────────────────────┐
│  Platform                                               │
│  ┌─────────────┐   ┌────────────┐   ┌──────────────┐  │
│  │ help_articles│   │ REST API   │   │ Shell.js     │  │
│  │ (SQLite)    │◄──│ /api/help  │◄──│ Help Panel   │  │
│  └─────────────┘   └────────────┘   └──────┬───────┘  │
│                                             │          │
└─────────────────────────────────────────────┼──────────┘
                                              │
         window.shellOpenHelp('slug')         │ Deep-link API
              ┌───────────────────────────────┘
              ▼
┌──────────────────┐
│  Product A/B     │
│  [?] ikon i UI   │
│  → öppnar hjälp  │
└──────────────────┘
```

**Datamodell (`help_articles`):**

| Kolumn | Typ | Beskrivning |
|--------|-----|-------------|
| `id` | INTEGER PK | Auto-increment |
| `slug` | TEXT UNIQUE | URL-vänligt ID för deep-linking |
| `title` | TEXT | Artikelrubrik |
| `product` | TEXT | Produktfilter (null = alla produkter) |
| `category` | TEXT | Kategorigruppering |
| `body_md` | TEXT | Artikelinnehåll i Markdown |
| `keywords` | TEXT | Sökord (kommaseparerade) |
| `sort_order` | INTEGER | Sortering inom kategori |
| `updated_at` | DATETIME | Senast uppdaterad |

**API:**

| Metod | Endpoint | Beskrivning |
|-------|----------|-------------|
| GET | `/api/help` | Lista alla artiklar (stödjer `?q=sök` och `?products=filter`) |
| GET | `/api/help/user?products=X,Y` | Artiklar filtrerade per användarens produkter |
| GET | `/api/help/slug/:slug` | Hämta artikel via slug (för deep-linking) |
| GET | `/api/help/:id` | Hämta artikel via ID |
| POST | `/api/help` | Skapa ny artikel |
| PUT | `/api/help/:id` | Uppdatera artikel |
| DELETE | `/api/help/:id` | Ta bort artikel |

**Shell Help Panel (i shell.js):**
- Öppnas via ? icon i shell-baren
- Sökfält med realtidsfiltrering (titel + keywords)
- Artiklar grupperade per kategori
- Produktbadge visar vilken produkt artikeln tillhör
- Markdown-rendering med stöd för rubriker, listor, kod, tabeller
- Back-knapp för att återgå till artikellistan

**Deep-linking från produkter:**
```javascript
// Produkten anropar globalt exponerad funktion:
window.shellOpenHelp('budget-entry');
```
- Öppnar hjälppanelen direkt på angiven artikel
- Fungerar från vilken produkt som helst som laddar shell.js
- Exempel: Product A har en ?-ikon i Budget Entry som deep-linkar till `budget-entry`-artikeln

**Admin-tab (Help) i Platform Admin:**
- Trädvy med alla artiklar grupperade per kategori
- Markdown-editor med live-preview
- Fält: titel, slug, kategori, produkt, sortering, nyckelord
- Toast-notifikation vid sparning

**AI Chat RAG-integration:**
- AI-chatten söker automatiskt i hjälpartiklar baserat på användarens fråga
- Matchande artiklar visas som klickbara kort i chatten
- Klick öppnar artikeln i hjälppanelen

**Framtida förbättringar:**
- 🔲 Bilduppladdning (upload till `/api/help/images` → markdown-referens)
- 🔲 Versionering av artiklar (revision history)
- 🔲 Rollbaserad synlighet (admin-only artiklar)

### Flöde
1. Användaren öppnar `localhost:3000` → ser login-sida
2. Loggar in → redirectas till sin **primära produkt** (admin → Platform Admin)
3. Shell header visas med navigering enbart till produkter man har tillgång till
4. Singel-produktanvändare ser minimal 4px bar, multi-produkt ser full navigering

## Produkt-UI Design

### Produkt-specifik toppbar
Varje produkt har en egen **sticky produkt-bar** under shell-baren med unik identitet:

| Produkt | Accent-färg | Ikon | Namn i bar |
|---|---|---|---|
| Product A | Blå `#4361ee` | 📊 | Budget & Planering |
| Product B | Lila `#7c3aed` | 📉 | Analys |
| Platform Admin | Mörk `#1a1a2e` | 🏗️ | Platform Admin — Master Data Hub |

### Product A: Multi-view layout
Product A har en **tvåvy-layout** med fliknavigering i produkt-baren:

| Vy | Innehåll |
|---|---|
| **Översikt** | Skapa projekt, projektlista, resiliens-info |
| **Budgetinmatning** | Spreadsheet-grid med 12 månadskolumner (Jan–Dec) |

**Budgetinmatning:**
- **Årsväljare** (2025/2026) ovanför grid
- **Inline-redigering**: Klicka direkt i cell → redigera belopp
- **Knapprad**: "+ Ny rad" (ghost), "💾 Spara utkast" (grön), "📤 Skicka in" (accent)
- **Versionsstatus**: Badge visar ✏️ Utkast eller ✅ Inskickad
- **Skrivskydd**: När status = inskickad → alla celler readonly, "Spara/Skicka in" disabled
- **CSS**: Spreadsheet-inspirerat — ljus gråblå bakgrund `#f4f6fc` på redigerbara celler, fokus-glow i accent-färg

### Burger-meny (sidebar)
Varje produkt har en ☰ hamburgermeny som öppnar en slide-in sidebar med dummy-navigation:
- **Product A**: Översikt, Projekt, Budget, Prognoser, Inställningar, Dokumentation
- **Product B**: Översikt, Budget vs Utfall, Projekt, Rapporter, Inställningar, Dokumentation
- Sidebar har produkt-branding med färg-dot och namn

### Ihopfällbara info-blocks
Alla info-blocks är **ihopfällda som default**. Varje block visar en klickbar trigger:
```
ℹ️ Visa förklaring ▸    (klicka för att expandera)
ℹ️ Dölj förklaring ▾    (klicka för att fälla ihop)
```
Enkel/Teknisk-toggle sitter i produkt-baren.

## Platform Admin

Admin-vy på `localhost:3000/admin.html` — åtkomlig via användare `admin/demo`.

### Flikstruktur

UI:t är organiserat i **åtta flikar** som speglar tre ansvarsområden — plattformsinfrastruktur, master data/domänregistrering, och domändrift:

| Flik | Syfte | Målgrupp | Nyckelord |
|---|---|---|---|
| **⚙️ Configuration** | Plattformsinfrastruktur — system, produkter, routing, integrationer | Leverantör (driftsättning) | Connected Systems, Event Subscriptions, External Tools |
| **📊 Master Data** | Domänregistrering + kors-domän referensdata | Kundadmin / domänexpert | Economy Domain, HR Domain, Shared Dimensions, Identity Linking |
| **🔧 Domains** | Domänspecifik pipeline och drift (med domän-selector) | Platform Ops | Pipeline Status, Sync, Validate, Publish, Recovery |
| **🔑 Identity & Access** | Användarhantering | Kundadmin | SCIM, lokal skapning, roller, grupp-mappning |
| **📋 Events** | Övervakning | Alla | Realtids event-logg |
| **🎬 Demo** | Testverktyg | Intern | Demo Runner + Golden Path |
| **❓ Help** | Hjälpartiklar — CRUD-editor med markdown + live-preview | Alla | Artiklar, kategorier, deep-linking |
| **📝 POC & Production** | Arkitektur & dokumentation | Intern | Architecture Vision + Production Notes |

Aktiv flik sparas i `localStorage` och behålls vid sidladdning.
Legacy-tabnamn (`setup`, `economy`, `operations`) mappas automatiskt till nya.

**Designprinciper:**
- **Configuration** = saker man ställer in en gång vid installation (infrastruktur-nivå)
- **Master Data** = hur kundens data ser ut — domäner, dimensioner, kopplingar (domän-nivå)
- **Domains** = allt om det löpande dataflödet — sync, publish, felhantering (drift-nivå)

**Master Data** organiseras i tre visuella sektioner:
1. **Data Domains** — Domänregistreringar med egna adapters (Economy ● aktiv, HR ○ placeholder)
2. **Cross-domain** — Dimensioner och routing som delas av alla domäner
3. **Identity & Entity Resolution** — Kanoniska entiteter, cross-system linking, planning tolkning

**Domains** har en domän-selector i headern. Varje domän har sin egen pipeline,
fakta-hantering och konfiguration. I nuläget finns en domän (Economy), men
arkitekturen stödjer fler (HR, Supply Chain, etc.) utan ny flik.

### External Tools

Plattformen stödjer konfigurerbara externa verktyg som visas i shell-barens navigation.

**Databas:**
```sql
external_tools (id, name, url, icon_url, sort_order, visible, created_at)
```

**API:**
| Endpoint | Beskrivning |
|---|---|
| `GET /api/external-tools` | Lista alla verktyg (admin) |
| `POST /api/external-tools` | Skapa nytt verktyg |
| `PUT /api/external-tools/:id` | Uppdatera verktyg (namn, url, sort) |
| `DELETE /api/external-tools/:id` | Ta bort verktyg |

**Shell-rendering:**
- `/api/navigation` returnerar `{ items: [...], externalTools: [...] }`
- 1–3 verktyg: visas som direktlänkar med ↗-ikon
- 4+ verktyg: de två första direkt, resten i "Tools ▾"-dropdown
- Admin-UI: tabell med Edit/Hide/Show/Delete + formulär för add/edit

### ⚙️ Configuration — Plattformsinfrastruktur

Mörk header-banner med **statuschecklista** (2 indikatorer som uppdateras live):

| Indikator | Grön när |
|---|---|
| 🖥️ Systems | ≥1 system aktiverat (via Connected Systems) |
| 🔀 Routing | ≥1 routing-regel |

**Connected Systems:**
Aktivera interna produkter (Product A, Product B) och visa aktiva system.
Varje system har `task_base_url` (för deep links i inbox) och `system_type` (erp/budgeting/analytics) från `system_config`.
ERP aktiveras automatiskt vid Economy Domain-staging. Interna produkter aktiveras manuellt via Enable-knappar.
Tabellen visar: System, Namn, Typ, Task-URL, med redigeringsmöjlighet.

**Event Subscriptions:**
Matris som kontrollerar vilka event-typer varje produkt tar emot. Toggling loggar blockerade leveranser.

**External Tools:**
Konfigurerbara länkar till externa system som visas i platform shell header.

### 📊 Master Data — Domäner & referensdata

Mörk header-banner med **statuschecklista** (5 indikatorer):

| Indikator | Grön när |
|---|---|
| 🏦 Economy Domain | ≥1 entitet i Economy Domain |
| 📚 Dimensioner | ≥1 delad dimension registrerad |
| 👥 Deltagare | Minst en dimension har deltagare |
| 🏷️ Kodlistor | ≥1 kod i någon dimension |
| 📐 Ekonomimodell | ≥1 dim-slot konfigurerad |

**Sektion: Data Domains**

Varje domän visas som ett kort med enable/sync och KPI:er.
Fler domäner (HR, Supply Chain) registreras på samma sätt — ny adapter + nytt kort.

- *Economy Domain* — Standardiserat staginglager (econ_*). Adapter transformerar ERP-data.
- *HR Domain* — Placeholder (ej aktiverad). Samma mönster med egen adapter.

**Sektion: Cross-domain**

- *Shared Dimensions* — Dimensioner delade mellan domäner/system (account, org_unit, project). Ägare, taxonomi, kodlistor, deltagare.
- *Economic Model & Routing* — Flex-dimensionsmodell (dim1=Aktivitet etc.) och routing-regler (source_field → target_slot).

**Sektion: Identity & Entity Resolution**

- *Projects (Canonical Entities)* — Tabell med alla canonical projects och deras ID-mappningar.
- *Link Entities* — Dropdown-baserat UI för att koppla ihop ERP ↔ Product A-projekt.
- *Planning Dimensions* — Hur Product A:s budget-versioner översätts till Product B:s planning-dimensioner.

### 🔧 Domains — Domänpipeline (drift)

Domän-selector i headern (Economy / HR coming soon).
All pipeline-data filtrekras per vald domän.

Innehåll per domän:
- **Pipeline Status** — KPI:er (entiteter, received, validated, published, rejected, DLQ)
- **Actions** — Validate, Publish, Full Sync
- **Recovery & Re-read** — Sync Period, Re-read Period, Full Re-read, Re-validate
- **Rejected Facts** — Avvisade rader med orsak (expanderbar)
- **Entities** — Entitetstabell filtrerad per dimension (expanderbar)
- **Sync State & Scheduler** — Watermark-tabell + cron-scheduler (expanderbar)
- **Pipeline Configuration** — Error policy, auto-publish (expanderbar)
- **Structural Policies** — Platform-level regler per dimension (expanderbar)
- **Attribute Publishing** — Kontroll av vilka attribut som publiceras downstream (expanderbar)

### 🔗 Operations → (borttagen — absorberad i Master Data + Domains)

### 📋 Events

Visar alla in/ut-events i realtid (pollar var 3:e sekund).
Varje rad: tidstämpel, riktning (IN/UT), topic, event-typ, canonical_id.
In-memory ring buffer (max 200 events) via `GET /api/events`.

### 🎬 Demo

Demo Runner med 11 klickbara steg. Steg 1 utför grundinställningen
automatiskt (dimensioner + ekonomimodell). Steg 11 demonstrerar process management. Resiliens demonstreras separat via Docker CLI. Alla flikar uppdateras live.

## Info-block i UI (Demo Explainer)

Varje åtgärd i UI:t (t.ex. "Skapa budgetprojekt", "Lägg budget") har ett **info-block** som förklarar vad som händer. Info-blocket har två lägen som användaren togglar mellan:

### Enkel förklaring (icke-teknisk)
```
ℹ️ Vad händer?
När du skapar ett budgetprojekt här i Product A registreras det
automatiskt i plattformen som sedan gör det synligt för andra
produkter som behöver informationen.
```

### Teknisk förklaring
```
⚙️ Tekniskt flöde
1. POST /api/projects → Product A skapar projekt lokalt (SQLite)
2. Product A publicerar BudgetProjectCreated → Kafka topic: product-a.events
3. Platform konsumerar → skapar canonical_id (platform-XXX) i mapper (SQLite)
4. Platform publicerar berikad event → Kafka topic: platform.projects.out
5. Product B konsumerar → sparar i sin lokala read model
Teknologier: Express, KafkaJS, Redpanda, SQLite, OpenTelemetry
```

### Toggle
En liten knapp/switch i varje info-block: **Enkel** | **Teknisk**
Valet sparas i localStorage så det behålls vid sidnavigering.

Varje knapp/åtgärd i UI:t ska ha ett sådant info-block:
- **Login-sidan**: JWT-autentisering, cookie, platform shell
- **Product A — Skapa projekt**: Event-flöde till Platform + Product B
- **Product A — Lägg budget**: Budgetevent, utkast/submit-modellen, berikning, routing
- **Product B — Analysgrid**: Hur read model byggs av events, planning-dimensioner
- **Platform — Länka projekt**: Merge av canonical IDs, ProjectLinked event
- **Platform — Dimensionsmappningar**: Semantisk översättning mellan domänmodeller
- **Platform — Demo Runner**: API-endpoints, steg-ordning, state-hantering

## Mappstruktur

```
platform-poc/
├── docker-compose.yml
├── package.json
├── PLAN.md
├── test-demo.ps1           # PowerShell: automatiserat 11-stegs demotest
├── shared/
│   └── events.ts          # Event-scheman (TypeScript types)
├── erp-mock/
│   ├── Dockerfile
│   ├── package.json
│   └── src/
│       └── index.ts
├── product-a/
│   ├── Dockerfile
│   ├── package.json
│   ├── src/
│   │   └── index.ts
│   └── public/
│       └── index.html      # UI: produkt-bar + sidebar + budget
├── product-b/
│   ├── Dockerfile
│   ├── package.json
│   ├── src/
│   │   └── index.ts
│   └── public/
│       └── index.html      # UI: produkt-bar + sidebar + analys
├── platform/
│   ├── Dockerfile
│   ├── package.json
│   ├── src/
│   │   ├── index.ts        # Express + auth + API + demo runner + shared-dimensions + economy domain
│   │   ├── mapper.ts       # SQLite: canonical ID, dimensions, dim_models, shared_dimensions, system_config, users
│   │   └── router.ts       # Kafka consumer/producer + event log + enrichment + dim routing (GL + budget)
│   └── public/
│       ├── login.html       # Login-sida (centrerad, ihopfällbar info)
│       ├── admin.html       # Platform Admin: 8 flikar (Configuration, Master Data, Domains, Identity, Events, Demo, Help, POC & Production)
│       ├── shell.js         # Gemensam header (pin/unpin, notch pill, inbox, help panel, AI chat, external tools)
│       └── architecture.png # Arkitekturbild för Architecture Vision-fliken
```

## Testcase / Demoflöde (11 steg)

Demon körs via Platform Demo Runner (admin-UI eller `test-demo.ps1`).
Steg 1–11 via API. Resiliens demonstreras separat via Docker CLI.

### Steg 1 — Grundinställning: Referensdata + ekonomimodell + dimensionskatalog
ERP → `AccountsPublished` på `erp.accounts`
```
accounts: [{ code: "4010", name: "Löner" }, { code: "4020", name: "Konsulter" }, { code: "5010", name: "Resor" }]
org_units: [{ code: "OU-100", name: "IT-avd" }, { code: "OU-200", name: "Ekonomi" }]
```
→ Platform konsumerar → vidarebefordrar på `platform.accounts.out`
→ Product A & B konsumerar → sparar lokalt

**Dessutom (engångskonfiguration):**
- Registrerar 3 delade dimensioner: `account`, `org_unit`, `project`
- Registrerar deltagare: ERP (producer), Product A (both), Product B (consumer)
- Populerar kanoniska kodlistor från ERP:s referensdata (3 konton, 2 org-enheter)
- Konfigurerar flex-dimensionsmodell: dim1=Aktivitet, dim2=Kostnadsbärare, dim3=Motpart
- Konfigurerar routing: ERP.activity→dim1, ERP.cost_bearer→dim2, ERP.counterpart→dim3
- Konfigurerar routing för budgetverktyg: prod_a.activity→dim1, prod_a.cost_bearer→dim2, prod_a.counterpart→dim3
- Registrerar flex-dimensioner i Economy Domain: activity (AKT-100/200/300), cost_center (KB-500/600), counterpart (MP-200/300)
- Konfigurerar `system_config` med task_base_url per produkt

### Steg 2 — SCIM-provisionering av användare
Identity Provider (IdP) provisionerar användare via SCIM 2.0:
```
POST /api/scim/v2/Users  (per användare)
  Anna Svensson — controller, OU-100, Product A + B
  Erik Lindberg — analyst, OU-200, Product B
  Calle Björk — controller, OU-100, Product A
```
→ Platform skapar/uppdaterar användare i `users`-tabellen
→ Användarna kan nu logga in och tilldelas budgetuppgifter
→ I produktion: SCIM-push från Azure AD, Zitadel eller Okta vid HR-förändringar

### Steg 3 — ERP skapar riktigt projekt
ERP → `ProjectCreated` på `erp.projects`
```
erp_id: "erp-042", name: "Nytt kontorshus"
```
→ Platform konsumerar → skapar canonical_id, mappar `erp-042 → platform-001`
→ Platform publicerar berikad event på `platform.projects.out`

### Steg 4 — ERP publicerar utfall (med flex-dimensioner)
ERP → `GeneralLedgerPublished` på `erp.general-ledger`
```
erp_id: "erp-042"
entries: [
  { account: "4010", org_unit: "OU-100", amount: 480000, currency: "SEK", period: "2025-01",
    activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
  { account: "4020", org_unit: "OU-100", amount: 210000, currency: "SEK", period: "2025-01",
    activity: "AKT-200", cost_bearer: "KB-501", counterpart: "MP-201" }
]
```
→ Platform berikar med `canonical_id` + **flex-dim routing** (activity→dim1, cost_bearer→dim2, counterpart→dim3)
→ `platform.gl.out` → Product B konsumerar → **ingestion pipeline** applicerar regler (derive dim2 från kontogrupp, default dim3) → sparar utfall

### Steg 5 — Product A skapar budgetprojekt
Product A → `BudgetProjectCreated` på `product-a.events`
```
prod_a_id: "prod_a-001", name: "Nytt kontorshus — planering"
```
→ Platform konsumerar → skapar canonical_id `platform-002`, mappar `prod_a-001 → platform-002`

### Steg 6 — Product A sparar budget (utkast)
Product A → `POST /api/budget` — sparar lokalt, **ingen Kafka-publicering**
```
prod_a_id: "prod_a-001", year: "2025"
lines: [
  { account: "4010", org_unit: "OU-100", amount: 500000, currency: "SEK", period: "2025-01",
    activity: "AKT-100", cost_bearer: "KB-500", counterpart: "MP-200" },
  { account: "4020", org_unit: "OU-100", amount: 200000, currency: "SEK", period: "2025-01",
    activity: "AKT-200", cost_bearer: "KB-600", counterpart: "MP-300" }
]
```
→ Skapar `budget_versions`-rad med status `utkast`
→ Sparar `budget_lines` med FK till versionen (inkl. flex-dim-fält)
→ Inget event publiceras — budgeten stannar lokalt tills den skickas in

### Steg 7 — Platform: Konfigurera dimensionsmappning
Admin konfigurerar hur Product A:s "Budget 2025" översätts till Product B:s planning-dimensioner.

```
POST /api/dimension-mappings/configure
  canonical_id: "platform-002"
  version_name: "Budget 2025"
  year: "2025"
  source_version_id: "ver-prod_a-001-2025"
```
→ Platform parsar versionens namn via konvention: `"Budget 2025"` → `planning_type=Budget, planning_year=2025, planning_version=1`
→ Sparas i `dimension_mappings`-tabell i platform.db
→ Admin kan justera manuellt i admin-UI:t (dropdown för planning_type: Budget/F1/F2/F3)
→ Mappningen finns redo att användas när budget skickas in i nästa steg

### Steg 8 — Product A skickar in budget
Product A → `POST /api/budget-versions/:id/submit`
→ Ändrar versionstatus `utkast → inskickad`
→ Publicerar `BudgetSubmitted` på `product-a.events`:
```json
{
  "event_type": "BudgetSubmitted",
  "prod_a_id": "prod_a-001",
  "version_id": "ver-prod_a-001-2025",
  "version_name": "Budget 2025",
  "year": "2025",
  "lines": [...]
}
```
→ Platform konsumerar → berikar med `canonical_id` + `planning_dimensions` (från steg 7) + `dim_values_per_line` (flex-dim routing)
→ Platform publicerar på `platform.budget.out`:
```json
{
  "canonical_id": "platform-002",
  "planning_dimensions": {
    "planning_year": "2025",
    "planning_type": "Budget",
    "planning_version": 1
  },
  "dim_values_per_line": [
    { "dim1": "AKT-100", "dim2": "KB-500", "dim3": "MP-200" },
    { "dim1": "AKT-200", "dim2": "KB-600", "dim3": "MP-300" }
  ],
  "original": { ... }
}
```
→ Product B konsumerar → **ingestion pipeline** applicerar regler (derive dim2 baserat på kontogrupp, default dim3) → sparar budget_lines med planning_year/type/version + berikade dims

### Steg 9 — Manuell länkning
Plattformen → `linkProjects(prod_a-001, erp-042)`
→ Mergar `platform-001` och `platform-002` till ett canonical ID
→ Publicerar `ProjectLinked` på `platform.links.out`
→ Product B:s budget_lines och gl_lines konsolideras under samma canonical_id

### Steg 10 — Product B visar analys

| Konto | Typ | Dim1 (Aktivitet) | Dim2 (KB) | Dim3 (Motpart) | Planning | Belopp |
|---|---|---|---|---|---|---|
| 4010 | Budget | AKT-100 | KB-500 | MP-200 | Budget 2025 v1 | 500 000 |
| 4010 | Utfall | AKT-100 | KB-500 | MP-200 | — | 480 000 |
| 4020 | Budget | AKT-200 | KB-600 | MP-300 | Budget 2025 v1 | 200 000 |
| 4020 | Utfall | AKT-200 | KB-600 | MP-300 | — | 210 000 |

Analysen visar nu **samma flex-dims för både budget och utfall**:
- **Flex-dims** (platform-side routing): Både ERP och Product A:s källfält routas till dim1/dim2/dim3
- **Planning dimensions** (platform-side enrichment): Platform översätter "Budget 2025" → year/type/version
- **Economy Domain**: Plattformen har ett standardiserat staginglager med alla kodlistor och hierarkier

### Steg 11 — Process Management: Budgetuppgifter

Använder befintlig budgetversion från steg 6 (Budget 2025):
→ Sätter org_root till "ACME" (hela organisationen)
→ Tilldelningar: Anna → DEPT-A (Marketing & Sales), Calle → OU-300 (IT)
→ `PUT /api/budget-versions/:id/open` — öppnar versionen
→ Publicerar `TaskAssigned` per tilldelning → `product-a.tasks`
→ Platform konsumerar → skapar inbox-items med deep links
→ Anna och Calles inbox visar uppgifter med kontext (version + org-enhet)
→ Klick i inbox → navigerar till Product A:s budgetinmatning med rätt kontext
→ Org-scope filtrerar budgetgridden till tilldelad org + underliggande enheter
→ "Mark complete" klarmarkerar uppgiften → read-only grid + TaskCompleted → inbox uppdateras

### Resiliens: Produkt nere → synkar ikapp

Demonstrerar att en produkt kan vara nere medan events flödar — och synkar ikapp automatiskt.

**Demo-kommandon:**
```powershell
# 1. Stoppa Product B
docker compose stop product-b

# 2. Gör förändringar medan Product B är nere:
#    - Skapa nytt budgetprojekt i Product A
#    - Lägg budget + skicka in
#    - Publicera GL från ERP

# 3. Starta Product B igen
docker compose start product-b

# 4. Se i loggen att den processar alla missade events
docker compose logs product-b --tail 20
```

**Varför det fungerar:**
- Redpanda (Kafka) lagrar alla meddelanden oavsett om konsumenten är uppe
- Varje consumer group (`product-b-consumer`) har en **offset** per topic/partition
- När Product B startar igen läser den vidare från sin offset → processar allt den missat

## Resiliens & Event Sourcing

### Kafkas garantier i denna POC
- **Meddelanden lagras** i Redpanda tills retention-tid löpt ut (default: 7 dagar)
- **Consumer offsets** spåras per consumer group — inga events missas vid omstart
- **Ordning garanteras** inom en partition (vi kör med 1 partition per topic)
- **At-least-once delivery** — en konsument kan få samma event mer än en gång vid krasch/omstart. Våra handlers är idempotenta (INSERT OR REPLACE, UPSERT-logik)

### Vad händer vid krasch av respektive tjänst?

| Tjänst | Vad händer vid nertid | Vid omstart |
|---|---|---|
| **ERP Mock** | Inga nya events produceras | Nya events kan produceras igen |
| **Platform** | Events köas i ingress-topics | Konsumerar ikapp, berikar och routar alla |
| **Product A** | Kan ej skapa budget, men events köas | Konsumerar missade events från platform.*.out |
| **Product B** | Analysvy fryser | Konsumerar alla missade events, uppdaterar read model |
| **Redpanda** | Allt stannar (enda SPOF i POC) | Alla tjänster reconnectar automatiskt |

## Budget-versionsmodell (Unified)

### 4-stegs statusflöde

Budgetversioner och processhantering är samlade i **en enda modell**. En budgetversion
är den centrala enheten — den lagrar data, har tilldelningar, och driver uppgifter.

```
  Skapa           Öppna           Skicka in         Publicera
  (draft)      (tasks → inbox)  (data → Kafka)    (lås version)
     ↓               ↓                ↓                ↓
┌──────────┐   ┌──────────┐    ┌──────────────┐   ┌──────────────┐
│  DRAFT   │──→│   OPEN   │───→│  SUBMITTED   │──→│  PUBLISHED   │
│          │   │          │    │              │   │              │
└──────────┘   └──────────┘    └──────────────┘   └──────────────┘
  Redigera      Tasks i         Data publicerad    Skrivskyddad
  tilldelning   inbox           till Kafka         (immutable)
                    ↑                │
                    └────────────────┘
                       Reopen
```

- **Draft** — Version skapad, tilldelningar kan redigeras, budgetdata kan sparas
- **Open** — Uppgifter publicerade till inbox, användare kan mata in budget
- **Submitted** — Budgetdata publicerad via Kafka till plattformen (BudgetSubmitted)
- **Published** — Version låst, skrivskyddad, granskningsuppgift skapad
- **Reopen** — Submitted/Published → Open (återöppna för redigering)

**Tabeller i Product A (SQLite):**

| Tabell | Syfte |
|---|---|
| `budget_versions` | id, prod_a_id, name, year, status (draft/open/submitted/published), org_root, created_at, opened_at, submitted_at |
| `budget_assignments` | id, version_id (FK), org_unit, user_id, user_name, status (pending/done), completed_at |
| `budget_lines` | version_id (FK), prod_a_id, account, org_unit, amount, currency, period, dim1, dim2, dim3 |

**API — Budgetdata:**
- `POST /api/budget` — Spara utkast (skapa/uppdatera version + rader, ingen Kafka)
- `GET /api/budget/:prodAId?year=YYYY` — Hämta budgetrader
- `GET /api/budget-versions/:prodAId` — Lista versioner för ett projekt
- `GET /api/budget-versions` — Lista alla budgetversioner (för process management)

**API — Statusövergångar:**
- `PUT /api/budget-versions/:id/open` — Draft/Submitted → Open (publicerar tasks)
- `POST /api/budget-versions/:id/submit` — Open → Submitted (publicerar BudgetSubmitted)
- `POST /api/budget-versions/:id/publish` — Submitted → Published (låser version)
- `POST /api/budget-versions/:id/reopen` — Published/Submitted → Open

**API — Tilldelningar:**
- `GET /api/budget-versions/:id/assignments` — Lista tilldelningar
- `POST /api/budget-versions/:id/assignments` — Bulk-uppdatera tilldelningar
- `PUT /api/budget-versions/:id/org-root` — Sätt organisationsscope
- `PUT /api/budget-assignments/:id/complete` — Markera tilldelning som klar (+ TaskCompleted)
- `PUT /api/budget-assignments/:id/reopen` — Återöppna klar tilldelning
- `PUT /api/budget-versions/:versionId/assignments/:org/complete` — Markera klar via version+org (från budgetuppgift)
- `DELETE /api/budget-versions/:id` — Ta bort draft-version

**Fördelar med unified model:**
- Ett enda koncept att förstå: "budgetversion" med tillhörande tilldelningar
- Statusflödet driver hela processen — från planering till publicering
- Tilldelningar sitter på versionen, inte i ett separat system
- Klarmarkering i budgetuppgiften uppdaterar assignment-status och inbox i realtid

## Planning-dimensionsmappning (implementerad)

### Problemet
Product A säger "Budget 2025". Product B har en rikare dimensionsmodell:
- `planning_year` (2025)
- `planning_type` (Budget, F1, F2…)
- `planning_version` (1, 2, 3…)

### Lösningen: Platform-side semantic translation
Plattformen äger översättningen mellan produkternas domänmodeller — precis som den mappar projekt-ID:n.

```
Product A: "Budget 2025"
        ↓
  Platform mapper: dimension_mappings-tabell
        ↓
Product B: { planning_year: "2025", planning_type: "Budget", planning_version: 1 }
```

### Flöde
1. **Steg 7 (demo):** Admin skapar/bekräftar dimensionsmappning via `POST /api/dimension-mappings/configure`
2. Platform auto-parsar versionsnamn via konvention: `"Budget 2025"` → Budget + 2025
3. Mappningen sparas i `dimension_mappings`-tabell (platform.db)
4. Admin kan justera manuellt via `PUT /api/dimension-mappings/:id` eller dropdown i admin-UI
5. **Steg 8:** När `BudgetSubmitted` routas, berikar router med `planning_dimensions` från tabellen
6. Product B lagrar dimensionerna i `budget_lines` (planning_year, planning_type, planning_version)

### Tabell: dimension_mappings

| Kolumn | Typ | Beskrivning |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `canonical_id` | TEXT | t.ex. `platform-002` |
| `source_version_id` | TEXT | t.ex. `ver-prod_a-001-2025` |
| `planning_year` | TEXT | `"2025"` |
| `planning_type` | TEXT | `"Budget"`, `"F1"`, `"F2"` |
| `planning_version` | INTEGER | 1, 2, 3… (auto-increment per type+year) |

### Auto-konvention
Versionsnamn parsas automatiskt:
- `"Budget 2025"` → `{ type: "Budget", year: "2025", version: 1 }`
- `"F1 2025"` → `{ type: "F1", year: "2025", version: 1 }`
- `"F2 2026"` → `{ type: "F2", year: "2026", version: 1 }`

Om samma type+year redan finns → version auto-ökar (Budget 2025 v1, v2, v3…)

### API

| Metod | Endpoint | Beskrivning |
|---|---|---|
| GET | `/api/dimension-mappings` | Lista alla mappningar |
| POST | `/api/dimension-mappings/configure` | Skapa/hitta mappning |
| PUT | `/api/dimension-mappings/:id` | Manuell override |

## Dimension Mapping Strategy (övrigt)

### Bakgrund
I denna POC använder alla produkter **samma dimensionskoder** (t.ex. konto `4010`, org-enhet `OU-100`) som ERP definierar. I verkligheten kan produkter ha egna interna koder som behöver översättas.

### Implementerat i POC: Dimensionsparitet (budget ↔ utfall)

Genom budget dim-routing har vi eliminerat dimensionsgapet. Både ERP (utfall) och Product A (budget) skickar flex-dim-fält som plattformen routar till samma dim-slots:

| System | Dimension | Exempel | Routing |
|---|---|---|---|
| **ERP Mock** | ✅ activity, cost_bearer, counterpart i GL-poster | `"AKT-100"`, `"KB-500"` | erp.activity→dim1 |
| **Product A** | ✅ activity, cost_bearer, counterpart i budgetrader | `"AKT-100"`, `"KB-500"` | prod_a.activity→dim1 |
| **Product B** | ✅ dim1, dim2, dim3 i BOTH budget_lines OCH gl_lines | samma värden | — |
| **Platform** | Routar via applyDimRouting per källa | dim_values_per_entry / dim_values_per_line | 6 regler |

I Product B:s analysgrid syns detta som en **Aktivitet-kolumn**:
- Rader från budget → <span style="background:#fff3e0;padding:2px 6px;border-radius:3px;font-size:11px;color:#e65100">SAKNAS</span> (orange tagg = default-ifylld)
- Rader från GL → <span style="background:#e8f5e9;padding:2px 6px;border-radius:3px;font-size:11px;color:#2e7d32">AKT-100</span> (grön tagg = äkta ERP-data)

### Rekommenderad approach: Shared Taxonomy (hybrid)

Plattformen äger en **kanonisk kodlista** per dimension. Varje produkt som använder egna koder registrerar en mappning till den kanoniska taxonomin.

```
ERP:  "4010"  ───┐
                  ├──→  Kanonisk: "4010 Löner"  ──→  Product B: "4010" (direkt)
Product A: "A-LÖNER" ─┘                              Product A: "A-LÖNER" (via mappning)
```

**Skalning:** N+M mappningar (N producenter + M konsumenter), inte N×M.

### Dimensionsregistrering

Varje delad dimension måste **explicit registreras** i plattformen med metadata och regler. Produktinterna dimensioner (t.ex. Product A:s "budget_category") behöver aldrig registreras.

| Egenskap | Beskrivning | Exempel |
|---|---|---|
| **Namn** | Dimensionens identifierare | `account`, `org_unit`, `project` |
| **Ägare (master)** | System som äger den kanoniska kodlistan | ERP |
| **Producenter** | System som skickar data med denna dimension | ERP, Product A |
| **Konsumenter** | System som tar emot data med denna dimension | Product A, Product B |
| **Taxonomi-typ** | Shared (gemensam kod) eller Mapped (per-produkt koder) | Shared / Mapped |
| **Mappningskrav** | Om produkter behöver per-produkt kodöversättning | Ja/Nej per produkt |

### Mappningsregler per produkt (när koder skiljer sig)

| Produkt | Dimension | Lokal kod | Kanonisk kod | Riktning |
|---|---|---|---|---|
| Product A | account | `A-LÖNER` | `4010` | Båda |
| Product A | account | `A-KONS` | `4020` | Båda |
| Product B | account | *(använder kanoniska koder direkt)* | — | — |

### Administrationsgränssnitt (implementerat)

Platform Admin har:
1. ✅ **Dimensionskatalog** — lista alla registrerade dimensioner med ägare, producenter, konsumenter
2. ✅ **Kodlistor** — visa den delade taxonomin per dimension (klick-för-detalj)
3. ✅ **Kodmappningar** — per produkt: vilka lokala koder mappar till vilka kanoniska (med formulär för manuell tillägg)
4. 🔲 **Valideringsregler** — varning om en produkt skickar en okänd dimensionskod (framtida)
5. 🔲 **Impact-analys** — vilka produkter påverkas om en kod ändras/tas bort (framtida)

## Process Management (Unified med Budget Versions)

### Översikt
Process Management är integrerat i budgetversionsmodellen — det finns ingen separat
"budgetrunda". En budgetversion **är** processen. Admin skapar en version, tilldelar
org-enheter till användare, och öppnar den. När versionen öppnas publiceras uppgifter
till användarnas inbox via Kafka.

Statusflödet beskrivs i **Budget-versionsmodell (Unified)** ovan.

### Org-root scoping

När en budgetversion skapas kan admin välja en **org_root** — en nod i organisationshierarkin.
Tilldelningsmatrisen visar då bara org-enheter som är ättlingar till den valda noden.
Om ingen org_root väljs visas alla org-enheter.

### Tilldelningsmatris

När en version väljs i Process Management visas en matris med org-enheter × användare.
Status kan ändras direkt i matrisen via Done/Reopen-knappar:

| Org Unit | Kod | Tilldelad användare | Status |
|---|---|---|---|
| Marketing & Sales | DEPT-A | Anna Svensson | ✔ Done [Reopen] |
| IT | OU-300 | Calle Björk | Pending [✔ Done] |

Användare hämtas från Platform via `GET /api/users?product=prod_a`.

### Task-events via Kafka

När en version öppnas (eller nya tilldelningar läggs till på en öppen version):
1. Product A publicerar ett `TaskAssigned`-event per tilldelning → `product-a.tasks`
2. Platform Router konsumerar → skapar inbox_item i SQLite
3. Inbox-item har `task_path` med query-parametrar: `/?version=X&org=Y`
4. Shell.js hämtar inbox, klick → navigerar till `task_base_url + task_path`
5. Product A läser URL-parametrar → öppnar budgetinmatning med rätt kontext

När en tilldelning markeras som klar:
1. Product A publicerar `TaskCompleted` → `product-a.tasks`
2. Platform Router konsumerar → uppdaterar inbox_item status till "done"
3. Shell-barens inbox-badge uppdateras (via `postMessage` + 5s polling)

### Budgetuppgift (Budget Task Context)

När en användare klickar på en budgetuppgift i inboxen öppnas Product A med kontext:

**Toolbar:**
```
┌──────────────────────────────────────────────────────────────────────┐
│ Budget 2025 │ Year [2025▾] │ Org Unit [Marketing & Sales (all)▾] │ Pending │ [✔ Mark complete] [✕] │
└──────────────────────────────────────────────────────────────────────┘
```

- **Versionsnamn** visas som etikett
- **Year** auto-sätts till versionens år
- **Org Unit dropdown** rooted vid tilldelad org — visar underliggande enheter
- **Status** (Pending/Completed) från assignment
- **Mark complete** — markerar tilldelningen som klar + publicerar TaskCompleted
- **✕ Dismiss** — rensar uppgiftskontexten, visar hela budgeten ofiltrerad

**Filtrering:**
- Budgetgridden visar **bara rader** vars org_unit matchar vald org eller dess ättlingar
- Org Unit-dropdownen i varje rad begränsas till scope
- Summor och tiles visar bara filtrerade rader
- Byte av org i toolbar-dropdownen filtrerar om direkt

**Read-only vid klarmarkering:**
- När uppgiften markerats som klar (assignmentStatus = done) blir gridden skrivskyddad
- Alla inputfält och selects disabled med vit bakgrund
- Add/Save-knappar låsta
- Samma låsning gäller vid status "published"

### Realtidsuppdatering

- **Save Assignments** → refreshar tilldelningsmatrisen + version-listan utan sidladdning
- **Status-toggle i matrisen** → publicerar TaskCompleted/reopens assignment + refreshar
- **postMessage** → shell-barens inbox uppdateras direkt (inte bara vid 5s-polling)

### Platform-stöd

| Endpoint | Beskrivning |
|---|---|
| `POST /api/users` | Skapa lokal användare (source: "local") |
| `GET /api/users?product=X` | Lista användare (från DB), filtrerade per produkt |
| `GET /api/users/:id` | Enskild användare (utan password_hash) |
| `PUT /api/users/:id` | Uppdatera användarattribut (roll, org, produkter, etc.) |
| `DELETE /api/users/:id` | Ta bort användare |
| `POST /api/scim/v2/Users` | SCIM 2.0: Provisionera ny användare (simulerar IdP-push). Använder `parseGroupClaims()` för automatisk roll/produkt/org-mappning via group-prefix (`role:X`, `product:X`, `org:X`). Hanterar username-kollision via upsert. |
| `PATCH /api/scim/v2/Users/:id` | SCIM 2.0: Uppdatera användare (via externalId) |
| `DELETE /api/scim/v2/Users/:id` | SCIM 2.0: Deprovisionera (soft-delete, status→deprovisioned) |
| `GET /api/external-tools` | Lista alla externa verktyg |
| `POST /api/external-tools` | Skapa externt verktyg |
| `PUT /api/external-tools/:id` | Uppdatera externt verktyg |
| `DELETE /api/external-tools/:id` | Ta bort externt verktyg |
| `GET /api/navigation` | Returnerar `{ items, externalTools }` för shell-rendering |
| `GET /api/inbox` | Inbox med deep links (task_base_url + task_path) |
| `PATCH /api/inbox/:id` | Markera inbox-item som done |
| `GET /api/me` | Returnera JWT-payload för inloggad användare |

**Tabeller i Platform (SQLite):**

| Tabell | Syfte |
|---|---|
| `users` | Användaridentitet (user_id, external_id, username, email, role, org_unit, products, groups, status, source, password_hash, last_login) |
| `external_tools` | Externa verktyg (id, name, url, icon_url, sort_order, visible, created_at) |
| `shared_dimensions` | Registrerade delade dimensioner (name, label, owner_system, taxonomy_type) |
| `dimension_participants` | Vilka produkter som producerar/konsumerar en dimension |
| `dimension_code_mappings` | Kodöversättning per produkt (local_code ↔ canonical_code) |
| `system_config` | Nyckel-värde-konfiguration per system (t.ex. task_base_url) |
| `econ_entities` | **Economy Domain** — referensdata (konton, org-enheter, projekt) |
| `econ_relations` | **Economy Domain** — hierarkier (parent-child) |
| `econ_attribute_defs` | **Economy Domain** — attributdefinitioner per dimension |
| `econ_entity_attributes` | **Economy Domain** — attributvärden per entitet |
| `econ_facts` | **Economy Domain** — transaktionsdata (GL, budget) i staging |
| `econ_sync_state` | **Economy Domain** — synkstatus per källa |
| `help_articles` | **Help Service** — artiklar med slug, markdown, kategori, produktfilter |

> **Arkitekturbeslut:** Economy Domain (econ_*) är enda sanningskälla för kodlistor, hierarkier och attribut. De gamla tabellerna `dimension_codes`, `dimension_attributes`, `dimension_code_attributes` och `dimension_hierarchy` har tagits bort — hierarkier lagras nu i `econ_relations`, attribut i `econ_attribute_defs` + `econ_entity_attributes`. Befintliga API-funktioner (`getDimensionCodes`, `getHierarchy` etc.) delegerar nu till econ_*-tabellerna via tunna wrappers.

---

## Git & Versionering

**Repo:** `gilljams/platform` på GitHub (privat)
**Branch:** `main` (enda branch, direkt push)
**Lokal sökväg:** `Platform POC/`

### Snabbkommandon

```powershell
# Committa alla ändringar
git add -A; git commit -m "beskrivning"

# Pusha till GitHub
git push

# Se status
git status --short

# Se senaste commits
git log --oneline -10

# Ångra senaste commit (behåll ändringar)
git reset --soft HEAD~1
```

### Workflow

1. Vi jobbar direkt på `main` (POC — ingen branch-strategi behövs)
2. Committa efter varje avslutad feature/fix
3. Pusha regelbundet till GitHub som backup
4. Commit-meddelanden på engelska, kortfattade

### .gitignore

Hanterar: `node_modules/`, `dist/`, `*.db`, `*.sqlite`, `*.log`, `.env`, editor-filer
| `dimension_participants` | Vilka produkter som producerar/konsumerar varje dimension |
| `dimension_code_mappings` | Per-produkt kodöversättning (local_code → canonical_code) |
| `system_config` | Nyckel-värde-konfiguration per system (system_name, config_key, config_value) |

**API-endpoints:**
- `GET /api/shared-dimensions` — alla dimensioner med deltagare + antal koder
- `POST /api/shared-dimensions` — registrera ny dimension
- `GET /api/shared-dimensions/:name/codes` — kodlista
- `POST /api/shared-dimensions/:name/codes` — lägg till kod
- `POST /api/shared-dimensions/:name/participants` — registrera deltagare
- `GET /api/shared-dimensions/:name/mappings` — kodmappningar
- `POST /api/shared-dimensions/:name/mappings` — lägg till mappning

> Demosteg 1 registrerar automatiskt 3 dimensioner (account, org_unit, project) med deltagare, populerar kodlistor från ERP:s referensdata, registrerar flex-dimensioner i Economy Domain, och konfigurerar system_config.

### Ekonomimodell & dimensionsdjup

Produkter kan ha **olika dimensionsdjup**. Product A kanske budgeterar på 5 dimensioner medan Product B analyserar på 8. När Product B tar emot budgetdata från Product A saknas dim6–dim8.

**Lösning:** Varje produkt definierar en **ekonomimodell** som anger vilka dimensioner som används, vilka som är obligatoriska/valfria, och defaultvärden för saknade dimensioner.

#### Exempel: Dimensionsmodell per produkt

| Dimension | Product A (Budget) | Product B (Analys) |
|---|---|---|
| dim1: konto | ✅ Obligatorisk | ✅ Obligatorisk |
| dim2: org_unit | ✅ Obligatorisk | ✅ Obligatorisk |
| dim3: projekt | ✅ Obligatorisk | ✅ Obligatorisk |
| dim4: period | ✅ Obligatorisk | ✅ Obligatorisk |
| dim5: valuta | ✅ Obligatorisk | ✅ Obligatorisk |
| dim6: aktivitet | ❌ Används ej | ✅ Valfri, default: `"SAKNAS"` |
| dim7: motpart | ❌ Används ej | ✅ Valfri, default: `"INTERN"` |
| dim8: anläggning | ❌ Används ej | ✅ Valfri, default: `null` |

#### Dataflöde med dimensionsgap

```
Product A skickar budget:
  { konto: "4010", org_unit: "OU-100", projekt: "P-001", period: "2024-Q1", valuta: "SEK", amount: 500000 }
                           ↓
  Platform vidarebefordrar "as-is" (berikar med canonical_id, men lägger inte till dimensioner)
                           ↓
Product B tar emot → applicerar sin ekonomimodell:
  { konto: "4010", org_unit: "OU-100", projekt: "P-001", period: "2024-Q1", valuta: "SEK",
    aktivitet: "SAKNAS", motpart: "INTERN", anläggning: null, amount: 500000 }
```

#### Princip: Consumer-side default-ifyllning

Varje konsumerande produkt ansvarar själv för att mappa inkommande data till sin ekonomimodell:
- **Plattformen** levererar data med de dimensioner källan hade — lägger inte till eller tar bort
- **Konsumenten** applicerar sin egen ekonomimodell (aktiva dimensioner + defaultvärden)
- **Fördel**: Produkter är oberoende — ny dimension i Product B kräver ingen ändring i Platform eller Product A
- **Defaultvärden** som `"SAKNAS"` gör att man kan filtrera/gruppera på "data utan denna dimension" vs "data med äkta värde"

#### Ekonomimodell som konfiguration

Ekonomimodellen kan registreras centralt i plattformen (för överblick) men **tillämpas lokalt** i varje produkt:

```typescript
// Exempel: Product B:s ekonomimodell (konfiguration)
const ekonomimodell = {
  dimensions: [
    { name: "konto",       required: true },
    { name: "org_unit",    required: true },
    { name: "projekt",     required: true },
    { name: "period",      required: true },
    { name: "valuta",      required: true },
    { name: "aktivitet",   required: false, default: "SAKNAS" },
    { name: "motpart",     required: false, default: "INTERN" },
    { name: "anläggning",  required: false, default: null }
  ]
};
```

> **Notera:** ERP:s GL-data kan ha fler dimensioner än Product A:s budget. Samma princip gäller — Product B tar emot båda datakällorna och applicerar sin ekonomimodell. Budget-rader får `aktivitet: "SAKNAS"`, GL-rader kanske har `aktivitet: "AKT-42"` från ERP.

### Branschreferenser
- **XBRL** (finansiell rapportering) — taxonomi + extensions
- **FHIR** (hälsovård) — CodeSystem + ConceptMap
- **dbt** (datamodellering) — shared marts + source mappings
- **Data Mesh** — federated governance med gemensamma standarder

---

## Deployment-arkitektur (produktion)

POC:n kör allt lokalt i Docker Compose. Här beskrivs hur det skulle se ut i produktion.

### Modell 1: Kubernetes per kund

Alla produkter i samma kluster. Enklast när allt är Hypergene-hostat.

```
┌─ Kubernetes Cluster (namespace: customer-acme) ─────────────┐
│  ┌──────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐ │
│  │ Platform │  │ Product A │  │ Product B │  │  Kafka    │ │
│  │ (pod×2)  │  │ (pod×3)   │  │ (pod×2)   │  │ (cluster) │ │
│  └──────────┘  └───────────┘  └───────────┘  └───────────┘ │
│       └──────────── Service mesh (Istio/Linkerd) ──┘        │
│  ┌──────────┐  ┌───────────┐                                │
│  │ Postgres │  │   IdP     │  (Zitadel / Azure AD)         │
│  └──────────┘  └───────────┘                                │
│  Ingress → acme.budget.hypergene.se, acme.analytics...      │
└──────────────────────────────────────────────────────────────┘
```

- Varje kund = egen namespace (eller eget kluster vid strikt isolering)
- Pods skalas oberoende (Product A × 5 under budgetperiod, Product B × 2)
- Intern kommunikation via Kubernetes Services + Kafka topics

### Modell 2: Hybrid (produkter på olika hosting)

Vanligaste scenariot — vissa produkter kör hos Hypergene, andra hos kund/partner.

```
┌─ Hypergene Cloud ──────────────┐     ┌─ Kundens datacenter ────┐
│  Platform, Product A, Kafka    │     │  Product C (extern)     │
│                                │     │  Kundens ERP            │
└──────────┬─────────────────────┘     └───────────┬─────────────┘
           │                                       │
           └───── API Gateway (mTLS) + Event Bridge ┘
```

| Utmaning | Lösning |
|----------|---------|
| Nätverksåtkomst | API Gateway (Kong, Azure APIM) med mTLS. Inga öppna portar — allt via HTTPS |
| Events till extern produkt | Event Bridge: HTTP-baserad push (webhook) eller Azure Event Hub som mellanlager |
| System-till-system-auth | OAuth2 client credentials (`client_id` + `client_secret`) per produkt |
| Användaridentitet (SSO) | Gemensam IdP-issuer. OIDC-token valideras via JWKS-endpoint |
| Data-isolering | `tenant_id` i varje request/event |

### Modell 3: Multi-tenant SaaS

Samma kodbas, delad infrastruktur. Data separeras via `tenant_id`.

- Databas: PostgreSQL med schema-per-tenant eller row-level security
- Kafka: topic-per-tenant (t.ex. `acme.product-a.events`)
- Billigare att driva, svårare när kunder kräver egen hosting

### POC → Produktion — mappning

| POC (Docker Compose) | Produktion |
|---------------------|-----------|
| `docker-compose.yml` | Kubernetes Helm charts |
| Redpanda (lokal Kafka) | Azure Event Hub / Confluent Cloud |
| `http://product-a:3002` | K8s Service (`product-a.svc.cluster.local`) |
| `http://localhost:3000/shell.js` | CDN (`https://shell.hypergene.se/v1/shell.js`) |
| Cookie JWT (demo) | OIDC-token via IdP, JWKS-validering |
| SCIM-simulator | Äkta SCIM från Azure AD / Zitadel |
| SQLite | PostgreSQL (per kund eller schema-per-tenant) |
| Demo-runner | CI/CD pipeline + Terraform |

### Arkitekturbeslut som håller

Dessa val i POC:n fungerar direkt i produktion:
- **Frikopplade produkter** — kommunicerar via events + API:er, inte direkt DB-access
- **Plattformen som router** — inga punkt-till-punkt-kopplingar mellan produkter
- **Delad dimensionskatalog** — kanoniska koder, per-produkt mappning
- **Shell.js som injicerbart script** — fungerar oavsett hosting-modell
- **SCIM + OIDC-mönster** — redan simulerat, ersätts med äkta IdP

## Nästa steg

### Klart ✅
- [x] Miljö: Docker, Node.js, WSL, Git
- [x] Designa testcase / demoflöde (10 steg)
- [x] Fas 1: Infra (docker-compose, Redpanda, Jaeger, shared types)
- [x] Fas 2: ERP Mock (3 endpoints: accounts, projects, GL med activity-dimension)
- [x] Fas 3: Platform (auth, mapper, router, shell.js, login.html)
- [x] Fas 4: Product A (budget CRUD, Kafka producer/consumer, UI med info-blocks)
- [x] Fas 5: Product B (read model, analytics med UNION ALL, UI med info-blocks)
- [x] Info-blocks med Enkel/Teknisk toggle i alla vyer
- [x] Resiliens-dokumentation + demo-case (Steg 10)
- [x] Produkt-specifika toppbars med egen accentfärg
- [x] Burger-meny (sidebar) i Product A och B
- [x] Ihopfällbara info-blocks (default collapsed)
- [x] Minimal shell bar för singel-produktanvändare (4px → hover expand)
- [x] Admin-användare (admin/demo)
- [x] Platform Admin (master data hub, länknings-UI, event-logg)
- [x] Login-sida centrerad och snyggt formaterad
- [x] Dimensionsgap-demo: ERP skickar `activity` i GL, Product A saknar den, Product B fyller i default `"SAKNAS"`
- [x] Product B:s analysgrid visar Aktivitet-kolumn med visuell skillnad (grön = äkta, orange = default)
- [x] Demo Runner: Platform Admin UI med klickbara steg 1-9 och live event-logg
- [x] Demo Runner: API-endpoints (POST /api/demo/step/1-9, GET /api/demo/state, POST /api/demo/reset)
- [x] Test-script (test-demo.ps1): PowerShell-skript som kör hela 10-stegs demon
- [x] Period-format: Ändrat från kvartalsformat (2024-Q1) till år-månad (2025-01)
- [x] Product A UI-redesign: Multi-view layout (Översikt + Budgetinmatning), spreadsheet-grid med 12 månadskolumner
- [x] Demo-steg omorganiserade: ERP-steg först (1-3), sedan Product A (4-7), sedan Platform/Product B (8-9)
- [x] Budget-versionsmodell: `budget_versions`-tabell med status utkast/inskickad, `POST /api/budget` sparar utkast (ingen Kafka), `POST /api/budget-versions/:id/submit` publicerar BudgetSubmitted
- [x] Product A frontend: Versionsindikator (✏️ Utkast / ✅ Inskickad), "Spara utkast" + "📤 Skicka in"-knappar, skrivskyddat grid vid inskickad status
- [x] Platform router: Hanterar både `BudgetUpdated` och `BudgetSubmitted` event-typer
- [x] Planning-dimensionsmappning: `dimension_mappings`-tabell i platform.db, auto-parsing av versionsnamn ("Budget 2025" → type/year/version)
- [x] Platform enrichment: Router berikar BudgetSubmitted med `planning_dimensions` vid routing till `platform.budget.out`
- [x] Product B: `budget_lines` utökad med planning_year/type/version-kolumner, analytics-query inkluderar dims
- [x] Platform Admin: Ny "Dimensionsmappningar"-panel med tabell + manuell override (dropdown)
- [x] Demo-steg 6: "Konfigurera dimensionsmappning" — explicit manuellt steg före budget-submit
- [x] Dimension mapping API: GET, POST configure, PUT update
- [x] Flex-dimensioner (Approach C): dim_models + dim_routing-tabeller, applyDimRouting() i router, dim1-dim3 i Product B
- [x] Demosteg 1 konfigurerar: flex-dim model (dim1=Aktivitet, dim2=KB, dim3=Motpart) + routing (ERP→Product B)
- [x] Product B: gl_lines med dim1/dim2/dim3-kolumner, analytics med dim-labels från Platform API
- [x] Platform Admin: Tab-baserad navigation (⚙️ Grundinställning, 🔗 Löpande drift, 📋 Events, 🎬 Demo)
- [x] Shared Dimension Catalog: Backend-tabeller (shared_dimensions, dimension_participants, dimension_code_mappings) + Economy Domain (econ_entities, econ_relations, econ_attribute_defs, econ_entity_attributes) som enda sanningskälla
- [x] Shared Dimension Catalog: 7 CRUD-funktioner i mapper.ts + 7 API-endpoints i index.ts
- [x] Dimensionskatalog UI: Klickbar tabell med kodlista, deltagare och kodmappningar
- [x] Demosteg 1: Registrerar 3 dimensioner (account, org_unit, project) + deltagare + kodlistor
- [x] Platform Admin: Omstrukturerad till 4 flikar (⚙️ Grundinställning, 🔗 Löpande drift, 📋 Events, 🎬 Demo)
- [x] Grundinställning-flik: "Ny kund? Börja här." med statuschecklista (7 indikatorer) + Economy Domain + Connected Systems + Dimensioner + Ekonomimodell & Routing
- [x] Löpande drift-flik: Projekt + Länkning + Planning-dimensioner (separerad från engångsinställningar)
- [x] Demosteg 1 omdöpt: "Grundinställning: Referensdata + ekonomimodell" — tydliggör engångskaraktären
- [x] Budget dim-routing: Platform applicerar applyDimRouting(”prod_a”,”prod_b”) per budgetrad → dim_values_per_line
- [x] Product A: budget_lines utökad med activity/cost_bearer/counterpart-kolumner
- [x] Product A: BudgetSubmitted-event inkluderar flex-dim-fält per rad
- [x] Product B: budget consumer läser dim_values_per_line → lagrar dim1/dim2/dim3 för budget
- [x] Analytics: Budget OCH utfall visar nu samma dim1/dim2/dim3 (inget dimensionsgap)
- [x] Economy Domain som enda sanningskälla: Connector Registry (connectors + connector_dimensions) ersatt med Economy Domain (econ_*) + system_config
- [x] Platform Admin: Economy Domain-översikt i Grundinställning (entity/relation/dimension-count + per-dimension tabell med shared/flex-taggar)
- [x] Setup-checklista: 7 indikatorer (economy domain, system, dimensioner, deltagare, kodlistor, ekonomimodell, routing)
- [x] Demosteg 1 registrerar flex-dimensioner i Economy Domain (activity, cost_center, counterpart) + system_config
- [x] Demosteg 1 konfigurerar routing för både ERP och Product A → Product B (6 regler)
- [x] Grundinställning omordnad: Steg 1=Economy Domain → Steg 2=Dimensioner → Steg 3=Ekonomimodell & Routing (logisk ordning)
- [x] Setup-checklista numrerad i logisk ordning: 1.Economy Domain → 2.Dimensioner → 3.Deltagare → 4.Kodlistor → 5.Ekonomimodell → 6.Routing
- [x] Formulär i varje setup-steg: dimension-registrering, deltagare, dim-modell, routing-regler
- [x] Dropdowns populeras från kända system (statisk lista) + dimension-katalogen
- [x] Transaktionsdatum: GL-poster har transaction_date TEXT, Product B lagrar + visar i analytics
- [x] Kodmappning i routing: applyDimRouting() översätter lokala koder → kanoniska via dimension_code_mappings
- [x] Dimensionsattribut: dimension_attributes + dimension_code_attributes-tabeller, 4 API-endpoints, admin-UI visar attribut i kodtabell
- [x] Dimensionshierarki: econ_relations-tabell (parent-child), API-endpoints, admin-UI drill-down dimension explorer med breadcrumb
- [x] Dimensionstyp: shared_dimensions utökad med dimension_type (flat/hierarchy/time/account), visas i admin-UI
- [x] Demo steg 1: Registrerar attribut (kontotyp, kontogrupp, region, nivå) + kodattribut + org-hierarki (OU→DIV)
- [x] shared/events.ts: GeneralLedgerEntry + BudgetLine utökade med flex-dim-fält, BudgetSubmitted tillagd
- [x] ERP-mock: Periodformat normaliserat (2024-Q1 → 2025-01), transaction_date tillagd på GL
- [x] Demo steg 9: Analytics visar transaction_date-kolumn (datum för utfall, — för budget)
- [x] Connected Systems: Aktiverings-UI för interna produkter, task_base_url-hantering, system_type från system_config
- [x] Economy Domain entity-dropdown: dynamiskt populerad från faktiska dimensioner (ersätter hårdkodad HTML)
- [x] SYSTEM_CAPABILITIES: Statisk array i admin.html som definierar routing-fält per system (ersätter borttagen cachedConnectors)
- [x] SCIM-provisionering: Demosteg 2 provisionerar användare via simulerad IdP-push (POST /api/scim/v2/Users)

### Kvar
- [ ] Fas 6: OpenTelemetry tracing (instrumentera alla 4 Node.js-tjänster, verifiera i Jaeger)
- [x] README.md med start/stopp/test-instruktioner

### Next — Plattformsförbättringar (identifierade vid genomlysning)

**✅ Prio 1 — Transaktionsdatum på GL** (IMPLEMENTERAD)
GL-poster från ERP har `transaction_date TEXT` (ISO 8601, t.ex. "2025-01-15").
Skiljer transaktionsdatum från bokföringsperiod (`period`).
Product B lagrar och visar `transaction_date` i analytics + demo step 9.

**✅ Prio 2 — Kodmappning i routing-pipeline** (IMPLEMENTERAD)
`applyDimRouting()` gör nu kodöversättning via `dimension_code_mappings` efter fält→slot-mapping.
Fallback: passthrough om ingen mappning finns (bakåtkompatibelt).

**✅ Prio 3 — Dimensionsattribut + hierarki** (IMPLEMENTERAD)
Nya tabeller: `econ_attribute_defs`, `econ_entity_attributes`. Hierarkier lagras i `econ_relations`.
`shared_dimensions` utökad med `dimension_type` (flat/hierarchy/time/account).
Demo: 4010 → `{kontotyp: kostnad, kontogrupp: personal}`, org-enhet OU-100 → `{region: Stockholm, parent: DIV-01}`.
API-endpoints för attribut + relationer. Admin-UI: drill-down dimension explorer med breadcrumb.

**Prio 4 — Tidsdimensionsmappning** (medel insats, stor demoeffekt)
Ny tabell: `time_granularities` (product, grain, format).
Routern mappar `transaction_date` / `period` → konsumentens önskade granularitet.
Product B vill se per månad, en styrelsevy vill se per kvartal.
Demo: *"ERP skickar dagstransaktioner, plattformen aggregerar till månader."*

**✅ Prio 5 — shared/events.ts synk** (IMPLEMENTERAD)
`GeneralLedgerEntry` utökad med `activity?`, `cost_bearer?`, `counterpart?`, `transaction_date?`.
`BudgetSubmitted` tillagd. `BudgetLine` utökad med flex-dim-fält.

**✅ Prio 6 — Periodformat-validering** (IMPLEMENTERAD)
ERP-mocken normaliserad: "2024-Q1" → "2025-01" i fallback-data.

**Prio 7 — Economy Domain → routing-förslag** (medel insats, medel demoeffekt)
Economy Domain kopplas starkare till dim_routing:
- Auto-föreslå routing-regler utifrån registrerade flex-dimensioner
- Validera att routing-regler matchar dimensioner i Economy Domain
- Visa varningar: *"prod_a har 'counterpart' i Economy Domain men saknar routing-regel"*

### ✅ Prio 8 — Error Policy + Auto-Publish (IMPLEMENTERAD)
Fullautomatisk publish-pipeline efter manuell setup av förutsättningarna.

**Error Policy per källa** — styr vad som händer vid valideringsfel:
- `skip_invalid` — publicera validerade rader, logga rejected (default)
- `abort_on_error` — blockera ALL publish om ≥1 rad rejected
- `threshold:N` — blockera om rejektandelen överstiger N%

**Auto-publish i sync-runner:**
- Sync → stage → validate → evaluate policy → publish (om tillåtet)
- Status `blocked` sätts om policy stoppar publish
- Fullständig loggning: `[SCHEDULER] Auto-publish: 312 facts published (policy: skip_invalid)`

**Rejection reason per rad:**
- `econ_facts.rejection_reason` lagrar varför en rad avvisades
- Exponeras via `GET /api/economy/facts?status=rejected`

**Admin UI:**
- "Pipeline Configuration" card i Data Operations
- Dropdown: error policy val
- Checkbox: auto-publish on/off
- Sparas till `system_config` per källa, persisterar över omstarter

**API:**
- `GET /api/economy/pipeline-config/:source` — läs aktuell config
- `PUT /api/economy/pipeline-config/:source` — sätt policy + auto-publish

---

### ✅ Prio 9 — Rejected Facts View (IMPLEMENTERAD)
Fullständig synlighet över avvisade rader direkt i Admin UI.

**Rejected Facts card i Data Operations:**
- Visar alla rader med `staging_status = 'rejected'` (max 200)
- Kolumner: Account, Org Unit, Period, Amount, Rejection Reason, Received
- Automatisk refresh efter validate/publish/sync-operationer
- Röd-markerad rejection reason per rad
- Tomt-state med grön ✓ om inga rejected finns
- Scrollbar vid >400px höjd

**API (existerande):**
- `GET /api/economy/facts?status=rejected&limit=200` — hämta rejected rows med `rejection_reason`

**XSS-skydd:**
- `escapeHtml()` utility för all user-data i tabellen

---

### ✅ Prio 10 — Event Subscriptions (IMPLEMENTERAD)
Central routing policy som styr vilka event-typer varje produkt tar emot.

**Modell:**
- `event_subscriptions`-tabell: `(product, event_type, enabled)`
- Event types: `accounts`, `gl`, `budget`, `projects`, `dimensions`, `entity-linked`
- Default: alla produkter prenumererar på allt (seeded vid första start)

**Router enforcement:**
- `publishEgress()` i router.ts kontrollerar `isTopicEnabledForProduct(product, eventType)`
- Blockerade leveranser loggas: `[ROUTER] Subscription gate: platform.budget.out blocked for [prod_a]`
- V1: observability + gating — konsumenter prenumererar fortfarande tekniskt men routern skickar inte

**Admin UI — "Event Subscriptions" card:**
- Matris: produkter × event types med checkboxar (toggle on/off)
- Real-time save vid klick
- Statusmeddelande vid ändring
- Placerad under "Connected Systems" i Integration-tabben

**API:**
- `GET /api/subscriptions` — hämta alla subscriptions
- `PUT /api/subscriptions` — `{ product, event_type, enabled }` — toggle on/off

**Mönster:**
- Inspirerat av AWS EventBridge rules + Confluent topic ACLs
- Separation: plattformen styr routing, produkterna styr ingestion
- Framtid (V2): dynamiska Kafka ACLs per consumer group

---

### Integration Pipeline Backlog (prioritetsordning)

| # | Åtgärd | Status | Effekt |
|---|--------|--------|--------|
| 1 | Error policy per källa (abort/skip/threshold) | ✅ Klar | Styr om felaktig data blockerar pipeline |
| 2 | Auto-publish i sync-runner respektera policy | ✅ Klar | Fullautomatisk pipeline efter setup |
| 3 | Rejected facts-vy i Admin UI med felorsak | ✅ Klar | Synlighet vid fel |
| 4 | DLQ-tabell + exponering i UI | ✅ Klar | Fånga Kafka/router-fel |
| 5 | Change detection (hash-diff på dimension snapshots) | ✅ Klar | Undvik onödiga publishes |
| 6 | Re-read UI-knappar (period/full/re-validate) | ✅ Klar | Praktisk felhantering utan API-calls |
| 7 | Sync audit trail koppla till befintliga audit events | ✅ Klar | Spårbarhet |
| 8 | Attribute versioning (SCD2 valid_from/valid_to) | 🔲 | Historik, tillbakablick |

**Best practice-referenser:**
- Idempotent ingestion med upsert + watermark (Fivetran, Airbyte)
- Error policy (abort/skip/threshold) (Snowflake `ON_ERROR`, dbt `store_failures`)
- DLQ + retry pattern (Confluent Kafka best practices)
- Change detection (hash-based publish) (dbt incremental models, Databricks Delta)
- Staging → validate → publish pipeline (ELT-pattern: Fivetran → Snowflake → dbt)
- Autonomous consumers (subscribe fromBeginning + dedupe) (Event Sourcing / CQRS)
- SCD Type 2 för dimensionshistorik (Kimball methodology)

### Framtida (utanför POC)
- [ ] Multi-tenant / organisationshantering
- [ ] **AI Assistant som plattformstjänst** — se nedan

---

### Implementerade pipeline-förstärkningar (2025-05-06)

**Pipeline Health Dashboard (ny)**
- API: `GET /api/pipeline-health` — aggregerar KPI:er från econ_facts, dead_letter_queue, sync_state, audit_events
- UI: Nytt dashboard-kort överst i Data Operations med 6 live-KPI:er:
  - Entities | Facts Published | Rejected | DLQ Pending | Skipped (no change) | Last Publish
- Dynamisk statusindikator: grön/amber/röd beroende på DLQ och rejected-status
- Uppdateras var 3:e sekund via poll-loopen

**Operational Actions — Backlog #6**
- *Re-read Period*: API `POST /api/economy/sync/:source/run?scope=facts&period_from=X&period_to=Y`
  Raderar gamla fakta för perioden och hämtar på nytt från ERP
- *Full Re-read*: API `POST /api/economy/sync/:source/full-reread`
  Nollställer watermark (high_watermark = NULL) och kör full sync
- *Re-validate*: API `POST /api/economy/facts/revalidate`
  Återställer rejected → received, kör validering igen (använd efter att referensdata fixats)
- UI: Eget kort "Operational Actions" med 3 färgkodade sektioner (lila/amber/grön)

**Golden Path — Full Lifecycle Demo**
- API: `POST /api/demo/golden-path` — kör 6 steg i sekvens:
  1. Full sync (entities + facts) → audit trail
  2. Re-sync entities → change detection (skipped)
  3. Inject ogiltiga GL-rader → rejected
  4. Re-validate (misslyckas — referensdata saknas)
  5. Fix referensdata + re-validate → partiell återhämtning
  6. Final publish → alla validerade fakta levereras
- UI: Nytt "Golden Path" kort i Demo-fliken med steg-för-steg-logg
- Demonstrerar ALLA pipeline-capabilities med ett klick

**Dead Letter Queue (DLQ) — Backlog #4**
- Tabell `dead_letter_queue` (id, topic, event_type, raw_message, error, status, created_at, retried_at)
- Router: catch i `eachMessage` skriver misslyckade meddelanden till DLQ istället för att tappa dem
- API: `GET /api/dlq` (lista + count), `POST /api/dlq/:id/retry` (markera retried)
- Admin UI: DLQ-kort i Events-fliken med röd vänsterkant, tabell med retry-knappar

**Change Detection — Backlog #5**
- `computeContentHash(data)`: SHA256 av sorterat JSON-data, trunkerad till 16 tecken
- Sync-runner: innan dimension-publish beräknas hash → jämförs med `sync_state.content_hash`
- Om hash matchar: loggar "unchanged, skipping publish" och hoppar över
- Om hash ändrats: publicerar snapshot + uppdaterar `content_hash` i sync_state

**Sync Audit Trail — Backlog #7**
- `insertAuditEvent()` anropas vid 4 nyckelmoment:
  1. Entity-synk (organisationsträd)
  2. Fact-synk (ekonomidata)
  3. Publish blockerad (pga error policy)
  4. Dimension-publish (snapshot till Kafka)
- Alla synkhändelser syns i Events-fliken med IN/UT-taggar

---

### AI Assistant — Framkomlig väg

Chatten i plattformens shell (sparkle-ikonen) är idag en placeholder. Vägen till en fungerande AI-tjänst:

**Varför plattformen äger detta:**
- Plattformen äger identitet — vet vilka produkter användaren har behörighet till
- Cross-product-frågor ("varför skiljer budget från utfall?") kräver data från flera produkter
- Governance (modellval, loggning, policies) hanteras centralt
- Produkterna förblir autonoma — de levererar kontext, äger inte LLM-anropet

**Arkitektur:**
```
Vector DB (namespace per produkt)
 ├── prod_a: hjälpdocs, arbetsflöden, budgetforklaringar
 ├── prod_b: rapportdefinitioner, analysbeskrivningar
 └── platform: konfiguration, ekonomidomän, dimensioner

Query Router
 • user.products → filtrera namespaces (behörighetsstyrning)
 • current_product → boost relevans för aktiv produkt
 • platform-namespace → alltid inkluderat

LLM Endpoint (extern eller self-hosted)
 • System prompt + RAG-kontext
 • Streaming response till shell-chatten
```

**Steg 1 (minimal viable):**
- `POST /api/ai/chat` — tar `{ message, history }`, injicerar plattformsdata som kontext
- Behörighetsfilter: `req.user.products` styr vilken kontext som inkluderas
- Ingen vektor-DB — systemprompten fylls med live-data (dimensioner, fakta-summary, config)

**Steg 2 (RAG):**
- Vector DB container (chromadb/pgvector) med namespace per produkt
- Varje produkt pushar docs via webhook: `POST /api/ai/index`
- Embedding-pipeline: chunking + vektorisering vid push
- Retrieval: `WHERE namespace IN (user.products)` vid query

**Steg 3 (agentic):**
- Tool-use / function calling — chatten kan utföra handlingar (skapa uppgift, visa rapport)
- Produkt-specifika tools registreras centralt, körs via produktens API

---

## Roadmap — Nästa steg

Identifierade förbättringsområden, sorterade i föreslagen prioritetsordning.

### Prio A — ERP Mock → Ekonomidomän (konceptuell refaktor)
**Insats:** Liten–medel | **Påverkan:** Konceptuell tydlighet

"ERP Mock" ger intrycket att vi mockar ett specifikt system. Det vi egentligen modellerar är ett **kanoniskt domänlager** med tillrättalagd data (kontoplan, org, GL). I verkligheten skulle connectors/adapters per källsystem mappa data till detta format.

- [ ] Rename `erp-mock` → `economy-domain` (eller `finance-domain`)
- [ ] Topics: `erp.accounts` → `economy.accounts`, `erp.general-ledger` → `economy.gl`
- [ ] Adaptertyp: `erp` → `economy-source` (eller behåll som adapter)
- [ ] Uppdatera docker-compose, all doku, demo-steg
- [ ] Info-blocks som förklarar: *"I verkligheten skulle det finnas en adapter som transformerar ERP-data till detta kanoniska format"*

### Prio B — Hårdkodade antaganden → konfiguration
**Insats:** Medel | **Påverkan:** Mer realistisk arkitektur

Identifiera och ersätta hårdkodade antaganden med konfigurerbar parametersättning.

- [ ] Genomlys alla tjänster efter hårdkodade URL:er, topic-namn, systemnamn
- [ ] Konfigurerbar topic-mappning (varje system deklarerar in/ut-topics)
- [ ] Miljövariabler/config för portar och URL:er (istället för `http://product-a:3002`)
- [ ] Demo-runner bör använda system_config URLs (inte hårdkodade)

### Prio C — Utökade datamodeller (planning_source / ledger-partitionering)
**Insats:** Medel–stor | **Påverkan:** Stor — möjliggör Prio D

Budget-ledgern behöver stöd för **partitionering per källa** så att kontoinmatning, drivare och extern beräkning kan samexistera utan att skriva över varandra.

- [ ] `budget_lines.planning_source` — t.ex. `manual`, `driver`, `excel`, `api`
- [ ] Replace-semantik per source: ny import av `excel`-rader ersätter alla `excel`-rader, lämnar `manual` orörda
- [ ] Totaler aggregeras över alla sources (UNION ALL i analytics)
- [ ] UI: visa source per rad/grupp i Product A, filtrera i Product B
- [ ] Platform: berika `planning_source` i `platform.budget.out`

### Prio D — Extern beräkning (Excel / custom / API)
**Insats:** Stor | **Påverkan:** Stor affärsnytta — kräver Prio C

Kunder som vill göra egna beräkningsmodeller i Excel (eller via kod/API) ska kunna:
1. Läsa ut nödvändig dimensionalitet (kontoplan, org, perioder) via API
2. Göra sin beräkning (Excel, Python, etc.)
3. Ladda upp resultatet i ett tillrättalagt format med egen `planning_source`
4. Ersätta/uppdatera i flera omgångar utan att röra manuell inmatning

- [ ] Export-endpoint: `GET /api/budget-versions/:id/export` (CSV/JSON med full dimensionalitet)
- [ ] Import-endpoint: `POST /api/budget-versions/:id/import` med `source`-parameter
- [ ] Valideringsregler: rätt konton, rätt perioder, rätt org-enheter
- [ ] UI: import/export-knappar i Product A, filval, resultatvisning
- [ ] Excel-mall som referens

### Prio E — Manuell genomlysning (alla 10 demo-steg utan automation)
**Insats:** Liten | **Påverkan:** Verifikation

- [ ] Verifiera att alla flöden kan köras manuellt via admin-UI (utan demo-runner)
- [ ] Dokumentera eventuella gap eller saknade formulär
- [ ] Säkerställ att reset + omstart ger ett rent tillstånd

### ✅ Users & Identity (IMPLEMENTERAD)
- [x] `users`-tabell i Platform SQLite (user_id, external_id, username, email, role, org_unit, products, groups, status, source, password_hash, last_login, synced_at)
- [x] Login läser från DB (inte hårdkodad array), demo-användare seedas vid startup/reset
- [x] SCIM 2.0-endpoints: POST/PATCH/DELETE `/api/scim/v2/Users`
- [x] REST CRUD: GET/PUT/DELETE `/api/users/:id`
- [x] Admin-flik "Users & Identity" med User Directory, redigering, SCIM-simulator, Token & Session
- [x] Info-blocks med produktionsarkitektur (OIDC, SAML2, SCIM) och vad som simuleras
- [ ] Schema Registry (Avro/Protobuf) för event-validering
- [ ] RBAC: Finkorning behörighetsstyrning per dimension/entitet
