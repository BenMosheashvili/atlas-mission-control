package com.atlassystems.missioncontrol.repository;

import com.atlassystems.missioncontrol.dto.BlastRadiusResponse.CriticalPath;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Values;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * InfraNodeRepository
 *
 * All graph compute runs inside Neo4j — zero graph traversal in the JVM.
 *
 * Three queries:
 *
 *   1. dijkstraCriticalPath   — APOC apoc.algo.dijkstra weighted by p99LatencyMs.
 *                               Returns the single shortest (fastest-to-fail) path
 *                               from origin to the most impactful reachable node.
 *
 *   2. findSecondaryBlastNodes — BFS downstream excluding the critical path nodes.
 *                               These are the "slower propagation" targets.
 *
 *   3. findUpstreamCallers     — Reverse BFS: who depends on origin?
 *
 * Raw Driver (not SDN OGM) — avoids reflection overhead on hot observability paths.
 * All reads routed to READ replicas via AccessMode.READ.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class InfraNodeRepository {

    private final Driver driver;
    private static final String DB = "neo4j";

    // ── 1. Dijkstra Critical Path ─────────────────────────────────────────────
    //
    // apoc.algo.dijkstra(startNode, endNode, relationshipQuery, weightProperty)
    //
    // We want the path from `origin` to the node with the LOWEST total latency
    // (= the one that will fail first when origin degrades).
    //
    // Strategy:
    //   a) Find all nodes reachable from origin via DEPENDS_ON (downstream)
    //   b) Run apoc.algo.dijkstra from origin to each candidate
    //   c) Return the path with the MINIMUM totalCost
    //
    // The relationship weight `p99LatencyMs` must be a float property on
    // each DEPENDS_ON edge — set by MetricEventConsumer whenever latency updates.
    //
    // Cypher contract:
    //   Nodes: (:InfraNode {nodeId, name, group})
    //   Edges: (:InfraNode)-[:DEPENDS_ON {p99LatencyMs: float}]->(:InfraNode)
    //
    public CriticalPath dijkstraCriticalPath(String originId) {
        // Step 1: collect all reachable downstream node IDs (cheap BFS)
        String collectQuery = """
            MATCH (origin:InfraNode {nodeId: $originId})
            MATCH (origin)-[:DEPENDS_ON*1..15]->(downstream:InfraNode)
            WHERE downstream.nodeId <> $originId
            RETURN DISTINCT downstream.nodeId AS targetId
            """;

        List<String> candidates;
        try (Session session = readSession()) {
            candidates = session.executeRead(tx ->
                tx.run(collectQuery, Values.parameters("originId", originId))
                  .list(row -> row.get("targetId").asString())
            );
        }

        if (candidates.isEmpty()) {
            log.warn("[InfraNodeRepo] No downstream nodes for originId={}", originId);
            return CriticalPath.builder()
                .nodes(List.of(originId))
                .totalLatencyMs(0)
                .build();
        }

        // Step 2: run Dijkstra to every candidate, pick minimum cost path
        // apoc.algo.dijkstra returns `path` (Path) and `weight` (total cost)
        String dijkstraQuery = """
            MATCH (origin:InfraNode {nodeId: $originId})
            MATCH (target:InfraNode {nodeId: $targetId})
            CALL apoc.algo.dijkstra(origin, target, 'DEPENDS_ON>', 'p99LatencyMs')
              YIELD path, weight
            RETURN [node IN nodes(path) | node.nodeId] AS pathNodes,
                   weight                               AS totalCost
            """;

        record PathResult(List<String> nodes, double cost) {}
        PathResult best = null;

        try (Session session = readSession()) {
            for (String targetId : candidates) {
                final String tid = targetId;
                List<PathResult> rows = session.executeRead(tx ->
                    tx.run(dijkstraQuery,
                           Values.parameters("originId", originId, "targetId", tid))
                      .list(row -> new PathResult(
                          row.get("pathNodes").asList(v -> v.asString()),
                          row.get("totalCost").asDouble()
                      ))
                );

                if (!rows.isEmpty()) {
                    PathResult candidate = rows.get(0);
                    if (best == null || candidate.cost() < best.cost()) {
                        best = candidate;
                    }
                }
            }
        }

        if (best == null) {
            log.warn("[InfraNodeRepo] Dijkstra returned no path from originId={}", originId);
            return CriticalPath.builder()
                .nodes(List.of(originId))
                .totalLatencyMs(0)
                .build();
        }

        log.info("[InfraNodeRepo] Critical path from {}: {} hops, {:.1f}ms",
                 originId, best.nodes().size() - 1, best.cost());

        return CriticalPath.builder()
            .nodes(best.nodes())
            .totalLatencyMs(best.cost())
            .build();
    }

    // ── 2. Secondary Blast Nodes ──────────────────────────────────────────────
    //
    // All downstream nodes NOT on the critical path.
    // These are reached via higher-latency paths — they will degrade
    // but more slowly than the critical path nodes.
    //
    public List<String> findSecondaryBlastNodes(String originId, List<String> criticalPathNodes) {
        String query = """
            MATCH (origin:InfraNode {nodeId: $originId})
            MATCH (origin)-[:DEPENDS_ON*1..15]->(downstream:InfraNode)
            WHERE downstream.nodeId <> $originId
              AND NOT downstream.nodeId IN $critPathIds
            RETURN DISTINCT downstream.nodeId AS nodeId
            ORDER BY downstream.nodeId
            """;

        try (Session session = readSession()) {
            return session.executeRead(tx ->
                tx.run(query, Values.parameters(
                    "originId",   originId,
                    "critPathIds", criticalPathNodes
                )).list(row -> row.get("nodeId").asString())
            );
        }
    }

    // ── 3. Upstream Callers ───────────────────────────────────────────────────
    //
    // Nodes that depend ON the origin (reverse DEPENDS_ON traversal).
    // These are the callers that will start receiving errors when origin fails.
    //
    public List<String> findUpstreamCallers(String originId) {
        String query = """
            MATCH (origin:InfraNode {nodeId: $originId})
            MATCH (caller:InfraNode)-[:DEPENDS_ON*1..15]->(origin)
            WHERE caller.nodeId <> $originId
            RETURN DISTINCT caller.nodeId AS nodeId
            ORDER BY caller.nodeId
            """;

        try (Session session = readSession()) {
            return session.executeRead(tx ->
                tx.run(query, Values.parameters("originId", originId))
                  .list(row -> row.get("nodeId").asString())
            );
        }
    }

    // ── 4. Node group lookup (for SlaSimulationService) ───────────────────────
    //
    // Returns the `group` property of a node (e.g. "data", "edge", "compute").
    // Used by SlaSimulationService to select the correct MTTR table entry.
    //
    public String findNodeGroup(String nodeId) {
        String query = """
            MATCH (n:InfraNode {nodeId: $nodeId})
            RETURN n.group AS group
            """;

        try (Session session = readSession()) {
            return session.executeRead(tx ->
                tx.run(query, Values.parameters("nodeId", nodeId))
                  .list(row -> row.get("group").asString())
            ).stream().findFirst().orElse("unknown");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Session readSession() {
        return driver.session(
            SessionConfig.builder()
                .withDatabase(DB)
                .withDefaultAccessMode(org.neo4j.driver.AccessMode.READ)
                .build()
        );
    }
}
