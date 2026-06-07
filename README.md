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

### Infrastructure (Docker)
```bash
docker run -d --name neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/atlas-secret \
  -e NEO4J_PLUGINS='["apoc"]' \
  neo4j:5

docker run -d --name redis -p 6379:6379 redis:7

docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:7.5.0
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
