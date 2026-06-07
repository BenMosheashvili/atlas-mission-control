# Atlas — Infrastructure Mission Control

**Goal:** Understand how large-scale systems detect, isolate, and recover from failures — before entering the industry.

---

<img width="883" height="420" alt="image" src="https://github.com/user-attachments/assets/d3bd2f07-ef26-4767-88f2-0a336fc256a1" />


## The Question I Wanted to Answer

When a server fails inside a company like Netflix or Amazon, engineers don't just ask *"what broke?"*

They ask: **"What else will break because of it — and in what order?"**

This project is my attempt to understand how monitoring systems answer that question in real time.

---

## What I Discovered Along the Way

### Why infrastructure is a graph, not a list

Early on I modeled services as a flat list. That broke immediately — because services depend on each other. `api-gateway` calls `auth-vault` which calls `db-cluster`. A flat list can't represent that. A graph can.

This is why companies like Google and Netflix store their service topology in graph databases. The question "who depends on who" is a graph traversal problem, not a SQL query.

### Why BFS wasn't enough

My first version used BFS to find the blast radius — all nodes reachable from the failing one. It worked, but it answered the wrong question.

BFS answers: *"who will be affected?"* — binary, yes or no.

The real question is: *"who will be affected first?"*

I replaced BFS with Dijkstra's algorithm, using P99 latency as the edge weight. Now the system finds the fastest path of failure propagation — the route through which an incident will cascade most quickly. A node with 8ms latency on the path is a bigger threat than one with 400ms — even if both are "reachable."

### Why the frontend shouldn't compute this

My second version ran Dijkstra in the browser. It worked fine for 10 nodes. It would crash for 10,000.

The right answer: move the computation to the server. Neo4j — a graph database — runs Dijkstra natively on data that already lives in the graph. The frontend receives a result, not a problem to solve.

This is the principle: **bring the compute to the data.**

### Why design matters for this kind of product

The users of a system like this are NOC operators — engineers watching dashboards at 3am when something breaks. They need information density, not aesthetics. No rounded corners, no whitespace, no gradients. Every pixel should carry data.

---

## Architecture Decisions & Evolution

For a formal record of architectural trade-offs, see the [Architecture Decision Log (ADRs)](docs/ARCHITECTURE.md).

### Evolutionary Roadmap

| Version | What changed | Why |
|---------|-------------|-----|
| v1 | Basic D3 topology, BFS blast radius, circle nodes | First question: can I visualize service dependencies? |
| v2 | Replaced BFS with Dijkstra weighted by P99 latency | BFS answers "who" — Dijkstra answers "who first" |
| v3 | Moved compute to backend — Spring Boot + Neo4j + Kafka | Frontend Dijkstra breaks at scale |
| v4 | Full FUI redesign — diamonds, sparklines, chaos panel | NOC operators need density, not beauty |

---

## Stack

| Layer | Technology | Description |
|-------|-----------|-------------|
| **Backend** | Spring Boot 3.2, Java 21 | High-throughput async events processing |
| **Graph DB** | Neo4j + APOC | Native Dijkstra pathfinding traversal |
| **Streaming** | Apache Kafka | Event ingestion & distribution |
| **Cache** | Redis | High-speed reactive latency caching |
| **Frontend** | React 18, D3 v7 | Subscription-based topology renderer |

---

## Status & Quick Start

**Status:** Proof of concept with simulated data. The ingestion/traversal pipeline is production-grade and fully functional; the only missing piece is a live production data source.

<details>
<summary><b>Setup & Run Locally (if you wish to try it anyway)</b></summary>

### Prerequisites
- Java 21+
- Node 18+
- Docker

### Infrastructure
```bash
docker compose up -d
```

### Backend
```bash
cd backend
./gradlew bootRun
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```
</details>

---

*Built to understand how systems fail — before being trusted to prevent it.*
