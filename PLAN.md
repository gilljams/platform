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
| Admin UI-struktur | 6 flikar: ⚙️ Initial Setup → 🔑 Identity & Access → 📊 Master Data → 🔗 Operations → 📋 Events → 🎬 Demo |
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
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   ERP Mock   │     │  Product A   │     │  Product B   │
│  (external)  │     │  (Budget &   │     │ (Analytics)  │
│              │     │   Planning)  │     │              │
└──────┬───────┘     └──────┬───────┘     └──────▲───────┘
       │                    │                    │
       │ ProjectCreated     │ BudgetProject      │ Konsumerar
       │ AccountsPublished  │ Created            │ events
       │ GeneralLedger      │ BudgetUpdated      │
       │ Published          │                    │
       ▼                    ▼                    │
┌─────────────────────────────────────────────────────────┐
│                     REDPANDA (Kafka)                     │
│                     Event Broker                         │
└──────────────────────────┬──────────────────────────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │   Platform Layer    │
                │  - Mapping Service  │
                │  - Event Router     │
                │  - Platform API     │
                └─────────────────────┘
```

## Komponenter (7 containers)

| Container | Teknologi | Port |
|---|---|---|
| `redpanda` | Redpanda | 19092 (Kafka), 8081 (Schema Reg), 8082 (Admin) |
| `redpanda-console` | Redpanda Console | 8080 (UI) |
| `jaeger` | Jaeger all-in-one | 16686 (UI), 4318 (OTLP) |
| `erp-mock` | Node.js/Express | 3001 |
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
  entry_id: string;
  erp_id: string;            // T.ex. "erp-042"
  org_unit: string;          // T.ex. "OU-100"
  account: string;           // T.ex. "4010"
  period: string;            // T.ex. "2025-01" (år-månad)
  amount: number;
  currency: string;          // T.ex. "SEK"
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
| `platform.links.out` | ProjectLinked | Product A, Product B |

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

Scriptet renderar en **gemensam header** överst i varje produkt med två lägen:

**Multi-produkt (anna, admin)** — full 48px bar:
```
┌─────────────────────────────────────────────────────────┐
│  Platform POC   Platform Admin │ Product A │ Product B  │   Anna ▾
└─────────────────────────────────────────────────────────┘
```

**Singel-produkt (erik, calle)** — minimal 4px accent-remsa, expanderar vid hover:
```
┌─────────────────────────────────────────────────────────┐
│▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬│  (4px, expanderar vid hover)
└─────────────────────────────────────────────────────────┘
```
- CSS-variabel `--shell-height` sätts så produkt-baren sticky-positionerar korrekt
- Navigeringslänkar visas **bara för produkter användaren har tillgång till**
- Entitlement-check: redirect om ej behörig
- Logout + användarinfo (namn, roll, org-enhet)

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

UI:t är organiserat i **fyra flikar** som speglar arbetsflödet vid kundinstallation:

| Flik | Syfte | Nyckelord |
|---|---|---|
| **⚙️ Grundinställning** | Engångskonfiguration per kund | Dimensioner, ekonomimodell, routing |
| **🔗 Löpande drift** | Daglig administration | Projekt, länkning, planning-dims |
| **📋 Events** | Övervakning | Realtids event-logg |
| **🎬 Demo** | Testverktyg | 11-stegs demoflöde |

Aktiv flik sparas i `localStorage` och behålls vid sidladdning.

### ⚙️ Grundinställning — "Ny kund? Börja här."

Mörk header-banner med **statuschecklista** (6 indikatorer som uppdateras live):

| Indikator | Grön när |
|---|---|
| 🏦 Economy Domain | ≥1 entitet i Economy Domain |
| 🖥️ System | ≥1 system aktiverat (via Connected Systems) |
| 📚 Dimensioner | ≥1 delad dimension registrerad |
| 👥 Deltagare | Minst en dimension har deltagare |
| 🏷️ Kodlistor | ≥1 kod i någon dimension |
| 📐 Ekonomimodell | ≥1 dim-slot konfigurerad |
| 🔀 Routing | ≥1 routing-regel |

**Steg 1 — Economy Domain (staginglager):**
Översikt av Economy Domain med antal entiteter, relationer och dimensioner.
Visar per-dimension-tabell med entitetsantal och shared/flex-taggar.
Economy Domain är ett standardiserat lager — adapters (t.ex. `runEconSync`) transformerar källdata till gemensamt format.
Plattformen är ERP-oberoende: nya datakällor behöver bara en adapter som skriver till econ_*-tabellerna.

**Steg 1b — Connected Systems:**
Aktivera interna produkter (Product A, Product B) och visa aktiva system.
Varje system har `task_base_url` (för deep links i inbox) och `system_type` (erp/budgeting/analytics) från `system_config`.
ERP aktiveras automatiskt vid Economy Domain-staging. Interna produkter aktiveras manuellt via Enable-knappar.
Tabellen visar: System, Namn, Typ, Task-URL, med redigeringsmöjlighet.

**Steg 2 — Registrera delade dimensioner:**
Tabell med alla registrerade dimensioner (namn, etikett, ägare, taxonomi-typ, antal koder, deltagare).
Klicka på en rad → expanderbar panel med kanonisk kodlista + kodmappningar per produkt.
Två formulär sida vid sida:
- Registrera ny dimension (namn, etikett, ägare från kända system, taxonomi shared/mapped)
- Lägg till deltagare (dimension-dropdown, produkt från kända system, roll producer/consumer/owner)

**Steg 3 — Ekonomimodell & Routing:**
Två tabeller sida vid sida:
- Dimensionsmodell — vad dim1-dim3 betyder (slot → etikett → produkt)
- Routing — hur källfält mappas till dim-slots (källa → slot → etikett)
Två formulär sida vid sida:
- Dimensionsmodell (produkt från kända system, slot dim1/dim2/dim3, etikett)
- Routing-regel (källa från kända system, fält, mål-produkt, → slot)

Alla steg markerade med "Engångsinställning"-badge.
Formulär-dropdowns populeras dynamiskt från kända system och dimensioner.

### 🔗 Löpande drift

**Projekt (Canonical Entities):**
Tabell med alla canonical projects och deras ID-mappningar.
Uppdateras automatiskt när ERP/Product A publicerar events.

**Länka projekt:**
Dropdown-baserat UI för att koppla ihop ERP ↔ Product A-projekt.
Anropar `POST /api/link`.

**Planning-dimensioner:**
Visar hur Product A:s budget-versioner översätts till Product B:s planning-dimensioner.
Auto-tolkning per budgetversion, manuell korrigering via dropdown (Budget/F1/F2/F3).

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
│       ├── admin.html       # Platform Admin: 4 flikar (Grundinställning, Löpande drift, Events, Demo)
│       └── shell.js         # Gemensam header (full/minimal beroende på entitlements)
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
| `GET /api/users?product=X` | Lista användare (från DB), filtrerade per produkt |
| `GET /api/users/:id` | Enskild användare (utan password_hash) |
| `PUT /api/users/:id` | Uppdatera användarattribut (roll, org, produkter, etc.) |
| `DELETE /api/users/:id` | Ta bort användare |
| `POST /api/scim/v2/Users` | SCIM 2.0: Provisionera ny användare (simulerar IdP-push) |
| `PATCH /api/scim/v2/Users/:id` | SCIM 2.0: Uppdatera användare (via externalId) |
| `DELETE /api/scim/v2/Users/:id` | SCIM 2.0: Deprovisionera (soft-delete, status→deprovisioned) |
| `GET /api/inbox` | Inbox med deep links (task_base_url + task_path) |
| `PATCH /api/inbox/:id` | Markera inbox-item som done |
| `GET /api/me` | Returnera JWT-payload för inloggad användare |

**Tabeller i Platform (SQLite):**

| Tabell | Syfte |
|---|---|
| `users` | Användaridentitet (user_id, external_id, username, email, role, org_unit, products, groups, status, source, password_hash, last_login) |
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

> **Arkitekturbeslut:** Economy Domain (econ_*) är enda sanningskälla för kodlistor, hierarkier och attribut. De gamla tabellerna `dimension_codes`, `dimension_attributes`, `dimension_code_attributes` och `dimension_hierarchy` har tagits bort. Befintliga API-funktioner (`getDimensionCodes`, `getHierarchy` etc.) delegerar nu till econ_*-tabellerna via tunna wrappers.

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
- [x] Dimensionshierarki: dimension_hierarchy-tabell, 2 API-endpoints, admin-UI visar hierarki-sektion
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
Nya tabeller: `dimension_attributes`, `dimension_code_attributes`, `dimension_hierarchy`.
`shared_dimensions` utökad med `dimension_type` (flat/hierarchy/time/account).
Demo: 4010 → `{kontotyp: kostnad, kontogrupp: personal}`, org-enhet OU-100 → `{region: Stockholm, parent: DIV-01}`.
7 nya API-endpoints. Admin-UI visar attribut i kodtabell + hierarki-sektion.

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

### Framtida (utanför POC)
- [ ] Multi-tenant / organisationshantering

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
