# Atlas Systems — Infra Mission Control

Infrastructure observability platform with real-time topology graph,
Dijkstra-weighted blast radius analysis, and Z-Score anomaly detection.

## Stack
| Layer      | Technology                          |
|------------|-------------------------------------|
| Backend    | Spring Boot 3.2, Java 21            |
| Graph DB   | Neo4j 5 + APOC plugin               |
| Cache      | Redis (Lettuce reactive client)     |
| Streaming  | Apache Kafka                        |
| Frontend   | React 18, D3 v7                     |

## Quick Start

### Prerequisites
- Java 21+
- Node 18+
- Docker (for Neo4j + Redis + Kafka)

### Infrastructure (Docker Compose)

You can spin up all the required infrastructure (Neo4j with APOC, Redis, and Kafka in KRaft mode) with a single command:

```bash
docker compose up -d
```


### Backend
```bash
cd backend
./gradlew bootRun
# Starts on http://localhost:8080
```

### Frontend
```bash
cd frontend
npm install
npm run dev
# Starts on http://localhost:5173
# API calls proxied to :8080 via vite.config.js
```

## Key API Endpoints

| Method | Path                                          | Description              |
|--------|-----------------------------------------------|--------------------------|
| GET    | /api/v1/infra/blast-radius/{nodeId}           | Dijkstra blast radius    |
| GET    | /api/v1/infra/observer/stream                 | SSE metric stream        |
| GET    | /actuator/health                              | Health check             |
| GET    | /actuator/prometheus                          | Prometheus metrics       |

## Architecture Decisions
See docs/ARCHITECTURE.md for full decision log.
