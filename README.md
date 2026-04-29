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

## Kom igång

### Alt 1: GitHub Codespaces (inget att installera)

1. Gå till [github.com/gilljams/platform](https://github.com/gilljams/platform)
2. Klicka **Code** → **Codespaces** → **Create codespace on main**
3. Vänta ~2 min — Docker Compose startas automatiskt
4. Klicka på port 3000 i "Ports"-panelen → Platform öppnas i browsern
5. Logga in med `admin` / `demo`

> Gratis: 60 timmar/månad på GitHub Free-plan.

### Alt 2: Lokalt med Docker

**Krav:** Docker Desktop (eller Rancher Desktop / Podman Desktop) + Git.

```bash
git clone https://github.com/gilljams/platform.git
cd platform
docker compose up -d --build
```

Öppna http://localhost:3000 — logga in med `admin` / `demo`.

### Tjänster

| Tjänst    | URL                    | Beskrivning |
|-----------|------------------------|-------------|
| Platform  | http://localhost:3000   | Admin UI, router, auth, shell |
| ERP Mock  | http://localhost:3001   | Simulerat källsystem |
| Product A | http://localhost:3002   | Budgetering & processhantering |
| Product B | http://localhost:3003   | Analys & P&L-rapportering |
| Kafka UI  | http://localhost:8080   | Redpanda Console (topics, messages) |
| Jaeger    | http://localhost:16686  | Distributed tracing |

### Demo-körning

1. Logga in med `admin` / `demo`
2. Gå till **Platform Admin** → fliken **Demo**
3. Kör steg 1–11 i ordning — varje steg publicerar events genom systemet
4. Växla till Product A och Product B för att se resultaten

## Dokumentation

- [PLAN.md](PLAN.md) — fullständig projektplan, beslut, API:er, datamodell
- [FLOWS.md](FLOWS.md) — dataflöden, Kafka-topics, routing
- [DESIGN-SYSTEM.md](DESIGN-SYSTEM.md) — UI-konventioner och komponenter
- [ARCHITECTURE-REVIEW.md](ARCHITECTURE-REVIEW.md) — arkitekturgenomgång och branschjämförelse
