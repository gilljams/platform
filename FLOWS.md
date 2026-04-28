# Platform POC — Dataflöden & Arkitektur

## Översikt

Plattformen är en event-driven arkitektur där data flödar mellan system via **Apache Kafka** (Redpanda).
Plattformen agerar som central router — den berikar, normaliserar och vidarebefordrar händelser mellan
källsystem (ERP) och konsumerande produkter (Product A, Product B).

```
┌──────────┐    Kafka     ┌──────────┐    Kafka     ┌──────────────┐
│ ERP Mock │  ────────►   │ Platform │  ────────►   │  Product A   │
│ :3001    │              │  Router  │              │  :3002       │
└──────────┘              │  :3000   │              └──────────────┘
                          └──────────┘    Kafka     ┌──────────────┐
                               │       ────────►   │  Product B   │
                               │                    │  :3003       │
                               │                    └──────────────┘
                          ┌──────────┐
                          │ Redpanda │  Kafka broker :19092
                          │ Console  │  UI :8080
                          └──────────┘
```

---

## Tjänster (Docker Compose)

| Tjänst             | Port  | Beskrivning                              |
|--------------------|-------|------------------------------------------|
| `redpanda`         | 19092 | Kafka-kompatibel broker (Redpanda)       |
| `redpanda-console` | 8080  | Webb-UI för att inspektera topics        |
| `jaeger`           | 16686 | Distributed tracing UI                   |
| `erp-mock`         | 3001  | Simulerat ERP-system (källsystem)        |
| `platform`         | 3000  | Plattform — router, admin, auth, demo    |
| `product-a`        | 3002  | Budgeting & Forecasting                  |
| `product-b`        | 3003  | Analytics & Reporting                    |

---

## Kafka-topics

### Ingress (från källsystem → Platform)

| Topic                  | Producent  | Innehåll                                       |
|------------------------|------------|-------------------------------------------------|
| `erp.accounts`         | ERP Mock   | Kontoplan + organisationsenheter                |
| `erp.projects`         | ERP Mock   | Projektupplägg                                  |
| `erp.general-ledger`   | ERP Mock   | GL-transaktioner (utfall) med flex-dimensioner  |
| `product-a.events`     | Product A  | BudgetProjectCreated, BudgetSubmitted           |
| `product-a.tasks`      | Product A  | TaskAssigned / TaskCompleted (uppgiftshantering)|

### Egress (Platform → konsumenter)

| Topic                    | Konsumenter     | Innehåll                                    |
|--------------------------|-----------------|---------------------------------------------|
| `platform.accounts.out`  | Product A/B     | Kontoplan + org-enheter (genomsluss)         |
| `platform.projects.out`  | Product A/B     | Projekt med canonical_id                     |
| `platform.budget.out`    | Product B       | Berikad budget med planning-dims + flex-dims |
| `platform.gl.out`        | Product B       | Berikade GL-poster med dim-routing           |
| `platform.links.out`     | Product A/B     | Projektlänkningar mellan system              |

---

## Huvudflöden

### 1. Referensdata (Kontoplan & Org-enheter)

```
ERP /api/publish-accounts
  → Kafka: erp.accounts
  → Platform Router: vidarebefordrar outtt
  → Kafka: platform.accounts.out
  → Product A: uppdaterar lokal kontoplan
  → Product B: uppdaterar lokal kontoplan
```

### 2. Projekt-skapande & Länkning

```
ERP /api/create-project
  → Kafka: erp.projects
  → Platform Router: skapar canonical_id, registrerar i dimension catalog
  → Kafka: platform.projects.out
  → Product B: lagrar projekt

Product A /api/projects (POST)
  → Kafka: product-a.events (BudgetProjectCreated)
  → Platform Router: skapar canonical_id
  → Kafka: platform.projects.out
  → Product B: lagrar projekt

Platform: linkProjects(prod_a_id, erp_id)
  → Kopplar båda till samma canonical_id
  → Kafka: platform.links.out
```

### 3. Budget-flöde (Product A → Product B)

```
Product A: Spara budget (lokal, ingen Kafka)
  → SQLite: budget_lines

Product A: Submit budget
  → Kafka: product-a.events (BudgetSubmitted)
  → Platform Router:
      - Slår upp canonical_id
      - Berikar med planning-dimensioner (year, type, version)
      - Applicerar flex-dim routing (activity → dim1, cost_bearer → dim2, etc.)
  → Kafka: platform.budget.out
  → Product B: lagrar i budget_lines med alla dimensioner
```

### 4. Utfall / GL-flöde (ERP → Product B)

```
ERP /api/publish-gl  (triggas via demo steg 3 eller "Fetch Actuals"-knapp)
  → Kafka: erp.general-ledger
  → Platform Router:
      - Slår upp canonical_id via erp_id
      - Applicerar dim-routing per GL-rad (activity → dim1, cost_bearer → dim2, counterpart → dim3)
      - OBS: Om canonical_id saknas → raden droppas med varning
  → Kafka: platform.gl.out
  → Product B:
      - Applicerar ingestion rules
      - Lagrar i gl_lines (account, org_unit, period, amount, dim1-dim5)
      - Tillgängligt via /api/analytics (UNION ALL med budget_lines)
```

**"Fetch Actuals"-knapp i Product B:**
```
Product B UI → POST http://platform:3000/api/fetch-actuals
  → Platform → POST http://erp-mock:3001/api/publish-gl
  → ERP genererar 312 GL-poster (26 konto×org × 12 månader)
  → Kafka-pipeline → Product B (ca 2 sek fördröjning)
```

### 5. Analytics-aggregering (Product B)

Product B:s `/api/analytics` returnerar en UNION ALL av:
- **Budget-rader** (`source = 'budget'`): från `budget_lines` JOIN `projects`
- **Utfallsrader** (`source = 'actual'`): från `gl_lines` JOIN `projects`

Grupperade per: `canonical_id, account, org_unit, period, dim1, dim2, dim3`

---

## Dimension-routing (Flex-dimensioner)

Plattformen använder ett flex-dimensionsystem där varje produkt kan ha upp till 5 namngivna dimensioner.
Dim-routing mappar källsystemets fältnamn till produktens flex-slots:

| Källa (ERP)   | Slot  | Product A namn | Product B namn |
|---------------|-------|----------------|----------------|
| `activity`    | dim1  | Activity       | Activity       |
| `cost_bearer` | dim2  | Cost Center    | Cost Center    |
| `counterpart` | dim3  | Counterpart    | Counterpart    |

Konfigureras i Platform via `configureDimModel()` och `configureDimRouting()`.

### 6. Process Management & Task-flöde (unified model)

Budgetversioner och processhantering använder samma tabell `budget_versions`.
Status-flöde: **draft → open → submitted → published** (reopen → open).

```
Admin skapar/väljer budgetversion i Product A (Process Management-vy)
  → budget_versions redan finns (skapas i Budget Entry)
  → Tilldelningsmatris: org-enheter × användare
  → POST /api/budget-versions/:id/assignments

Admin öppnar version:
  → PUT /api/budget-versions/:id/open
  → Per tilldelning: TaskAssigned → Kafka: product-a.tasks
  → Platform Router: skapar inbox_items i SQLite
  → task_path: /?version=X&org=Y

Användare (Anna/Calle) ser uppgifter i shell-bar inbox:
  → GET /api/inbox (filtrerat per user_id från JWT)
  → resolved_link = task_base_url + task_path
  → Klick → navigerar till Product A med query-params

Product A läser URL-params → öppnar Budget Entry med uppgiftskontext:
  → taskContext = { version, org, assignmentStatus, versionName }
  → Org-scope dropdown (tilldelad org som toppnod, underliggande valbara)
  → Budgetgrid filtreras per org-scope
  → Låst (read-only) om uppgiften är klarmarkerad

Klarmarkering:
  → PUT /api/budget-versions/:versionId/assignments/:org/complete
  → TaskCompleted → Kafka: product-a.tasks
  → Platform Router: uppdaterar inbox_item.status = 'done'
  → postMessage('refresh-inbox') → shell uppdaterar badge
```

---

## Demo-steg

Plattformens admin-sida erbjuder 10 demo-steg som illustrerar hela flödet:

| Steg | Namn                           | Vad händer                                                            |
|------|--------------------------------|-----------------------------------------------------------------------|
| 1    | Initial Setup                  | ERP publicerar kontoplan + org; Platform konfigurerar dim-modell      |
| 2    | ERP: Create Project            | ERP skapar projekt "New Office Building"                              |
| 3    | ERP: Publish Actuals (GL)      | ERP publicerar 312 GL-poster (helår 2025, alla konton & org)         |
| 4    | Product A: Create Budget       | Product A skapar budgetprojekt                                        |
| 5    | Product A: Save Budget (draft) | Product A sparar budget lokalt (ingen Kafka)                          |
| 6    | Planning Dimensions            | Platform skapar planerings-dimensioner (Budget 2025)                  |
| 7    | Product A: Submit Budget       | Budget skickas → Kafka → Platform berikar → Product B                |
| 8    | Platform: Link Projects        | ERP-projekt ↔ Budget-projekt kopplas via canonical_id                |
| 9    | Product B: Show Analytics      | Product B visar budget vs utfall                                      |
| 10   | Process Management             | Väljer befintlig version, sätter org-rot, tilldelar Anna & Calle, öppnar → inbox-tasks |

---

## Autentisering & Navigation

### Demo-användare

| Användarnamn | Roll       | Produkter              | Primär produkt |
|-------------|------------|------------------------|----------------|
| `admin`     | admin      | platform, prod_a, prod_b | platform      |
| `anna`      | controller | prod_a, prod_b         | prod_a         |
| `erik`      | analyst    | prod_b                 | prod_b         |
| `calle`     | controller | prod_a                 | prod_a         |

Alla har lösenord `demo`. Användare lagras i Platform SQLite (`users`-tabell), seedas vid startup.

### Auth-flöde

1. `POST /api/login` → validerar mot `users`-tabellen → JWT-token i cookie `platform_token` (8h TTL)
2. JWT innehåller: `user_id`, `name`, `role`, `org_unit`, `products`, `primary_product`
3. `GET /api/navigation` → returnerar meny-items filtrerade på användarens `products` + anslutna system
4. Shell-baren (injicerad via `shell.js`) renderar navigationen dynamiskt

**Produktionsmodell (simulerad i POC):**
- Autentisering via **OIDC/SAML2** → redirect till extern IdP (Zitadel, Azure AD)
- Användarprovisionering via **SCIM 2.0**: `POST /api/scim/v2/Users` (IdP pushar create/update/deactivate)
- Auktorisering: produkttillgång + org-tillhörighet i `users`-tabell, produkter frågar via `GET /api/users`

### Shell-bar (Cross-product)

Shell-baren laddas via `<script src="http://localhost:3000/shell.js">` på alla produktsidor.
Den hanterar:
- Pinning/unpinning (göms om bara en produkt)
- Navigation mellan produkter
- Inbox med uppgifter (deep links till rätt produkt med kontext)
- Utloggning

### Inbox & Deep Linking

Inbox-items lagras i Platform (SQLite). Varje item har:
- `assigned_to` — filtrerar per inloggad användare
- `task_path` — relativ sökväg med query-params (t.ex. `/?version=ver-A0001-2025&org=DEPT-A`)
- `resolved_link` — Platform slår upp `task_base_url` från connector-registret och bygger full URL

När användaren klickar på en inbox-uppgift:
1. Shell.js navigerar till `resolved_link` (t.ex. `http://localhost:3002/?version=ver-A0001-2025&org=DEPT-A`)
2. Product A:s `handleUrlParams()` läser query-params → `initTaskContext(versionId, orgUnit)`
3. Hämtar version-info & assignment-status → sätter `taskContext`
4. Populerar org-scope dropdown (tilldelad org som rot)
5. Öppnar Budget Entry med filtrerat grid (read-only om uppgiften redan klar)
6. Klarmarkering: PUT → TaskCompleted → inbox uppdateras via postMessage
