# Architecture Decision Log

## ADR-001: Neo4j over PostgreSQL
Graph traversal for blast radius requires multi-hop DEPENDS_ON traversal.
PostgreSQL recursive CTEs degrade at scale. Neo4j traversal is O(edges).

## ADR-002: Dijkstra over BFS
BFS answers "who is reachable?" (binary).
Dijkstra with weight=p99LatencyMs answers "who fails fastest?" (prioritised).
Edge weight = target node's current P99 latency (from MetricsStore).

## ADR-003: Zero React re-render for topology
D3 subscribes directly to MetricsStore singleton.
Graph node colors mutate via d3.select().attr() — never triggers React reconciler.
MetricCards use useSyncExternalStore with per-node subscriptions.
Update to node X triggers re-render of card X only.

## ADR-004: Kafka batch mode + manual ACK
Consumer thread never blocks.
CompletableFuture fan-out: Redis write + anomaly eval run in parallel.
ACK fires in whenComplete — only after full batch is processed.

## ADR-005: Z-Score Sliding Window
ArrayDeque(60 samples) per nodeId:metricType key.
Synchronized on the specific Deque — fine-grained locking, not global.
stdDev < 1e-9 guard prevents NaN on constant-value metrics.

## ADR-006: MTTR by node group
MTTR is a property of failure MODE, not instance.
Table: data=45m, security=35m, compute=25m, infra=20m, edge=12m, observe=8m
Cascade bonus: +5m per hop (remediation complexity grows with blast depth).
