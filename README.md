# Platform POC

Proof-of-concept för en event-driven plattform där data flödar mellan system via **Kafka (Redpanda)**.

## Arkitektur

```
ERP Mock  ──►  Platform Router  ──►  Product A (Budgeting)
                                ──►  Product B (Analytics)
```

- **Platform** — central router, admin UI, auth, shell-bar, inbox
- **Product A** — budgetering, processhantering, uppgiftsflöde
- **Product B** — analys, P&L-rapportering (budget vs utfall)
- **ERP Mock** — simulerat källsystem (kontoplan, org, GL-data)

## Tech stack

Node.js · TypeScript · SQLite · Kafka/Redpanda · Docker Compose

## Starta

```bash
docker compose up --build
```

| Tjänst    | URL                    |
|-----------|------------------------|
| Platform  | http://localhost:3000   |
| Product A | http://localhost:3002   |
| Product B | http://localhost:3003   |
| Kafka UI  | http://localhost:8080   |
| Jaeger    | http://localhost:16686  |

Logga in med `admin` / `demo` och kör demo-stegen i Platform Admin.

## Dokumentation

- [PLAN.md](PLAN.md) — fullständig projektplan, beslut, API:er, datamodell
- [FLOWS.md](FLOWS.md) — dataflöden, Kafka-topics, routing
- [DESIGN-SYSTEM.md](DESIGN-SYSTEM.md) — UI-konventioner och komponenter
