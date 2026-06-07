package com.atlassystems.missioncontrol.controller;

import com.atlassystems.missioncontrol.dto.BlastRadiusResponse;
import com.atlassystems.missioncontrol.repository.InfraNodeRepository;
import com.atlassystems.missioncontrol.service.SlaSimulationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * BlastRadiusController
 *
 * GET /api/v1/infra/blast-radius/{originNodeId}
 *
 * Orchestration flow:
 *   1. Dijkstra critical path           ← Neo4j / APOC (InfraNodeRepository)
 *   2. Secondary blast nodes            ← BFS downstream minus crit path
 *   3. Upstream callers                 ← Reverse BFS
 *   4. Node group lookup                ← for MTTR selection
 *   5. MTTR + Impact score              ← SlaSimulationService
 *   6. Assemble BlastRadiusResponse     ← exact JSON contract
 *
 * Error handling:
 *   • Unknown nodeId          → 404 with structured error body
 *   • Neo4j unreachable       → 503 (Spring's default DataAccessException handler)
 *   • Unexpected exception    → 500 logged at ERROR level
 *
 * No business logic lives in this class — controller is pure orchestration.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/infra")
@RequiredArgsConstructor
public class BlastRadiusController {

    private final InfraNodeRepository infraNodeRepository;
    private final SlaSimulationService slaSimulationService;

    // ── GET /blast-radius/{originNodeId} ─────────────────────────────────────

    @GetMapping("/blast-radius/{originNodeId}")
    public ResponseEntity<BlastRadiusResponse> getBlastRadius(
            @PathVariable String originNodeId) {

        log.info("[BlastRadiusController] Blast radius requested for originNodeId={}", originNodeId);

        // 1. Dijkstra — critical path (fastest failure propagation)
        BlastRadiusResponse.CriticalPath criticalPath =
            infraNodeRepository.dijkstraCriticalPath(originNodeId);

        if (criticalPath.getNodes().isEmpty() ||
            criticalPath.getNodes().equals(List.of(originNodeId))) {
            log.warn("[BlastRadiusController] No downstream path found for originNodeId={}", originNodeId);
            // Still a valid response — isolated node with no downstream blast
        }

        // 2. Secondary blast nodes (all downstream, excluding critical path)
        List<String> secondaryBlastNodes =
            infraNodeRepository.findSecondaryBlastNodes(originNodeId, criticalPath.getNodes());

        // 3. Upstream callers (reverse traversal)
        List<String> upstreamCallers =
            infraNodeRepository.findUpstreamCallers(originNodeId);

        // 4. Node group — drives MTTR table selection
        String nodeGroup = infraNodeRepository.findNodeGroup(originNodeId);
        if ("unknown".equals(nodeGroup)) {
            log.warn("[BlastRadiusController] Node group not found for originNodeId={} — MTTR fallback active", originNodeId);
        }

        // 5. Cascade depth = critical path hops beyond origin (hops, not node count)
        int cascadeDepth = Math.max(0, criticalPath.getNodes().size() - 1);

        // 6. MTTR + impact score
        int mttrMinutes  = slaSimulationService.computeMttr(nodeGroup, cascadeDepth > 0, cascadeDepth);
        int totalAffected = criticalPath.getNodes().size() - 1 + secondaryBlastNodes.size();
        int impactScore   = slaSimulationService.computeImpactScore(
            criticalPath.getTotalLatencyMs(),
            totalAffected
        );

        // 7. Assemble response
        BlastRadiusResponse response = BlastRadiusResponse.builder()
            .originId(originNodeId)
            .impactScore(impactScore)
            .estimatedMttrMinutes(mttrMinutes)
            .criticalPath(criticalPath)
            .secondaryBlastNodes(secondaryBlastNodes)
            .upstreamCallers(upstreamCallers)
            .build();

        log.info("[BlastRadiusController] Response assembled: origin={} impact={} mttr={}m critPath={} secondary={} upstream={}",
                 originNodeId, impactScore, mttrMinutes,
                 criticalPath.getNodes().size(), secondaryBlastNodes.size(), upstreamCallers.size());

        return ResponseEntity.ok(response);
    }

    // ── Exception handlers ────────────────────────────────────────────────────

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<ErrorBody> handleNotFound(NoSuchElementException ex) {
        log.warn("[BlastRadiusController] Node not found: {}", ex.getMessage());
        return ResponseEntity.status(404)
            .body(new ErrorBody("NODE_NOT_FOUND", ex.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorBody> handleGeneric(Exception ex) {
        log.error("[BlastRadiusController] Unexpected error during blast radius computation", ex);
        return ResponseEntity.status(500)
            .body(new ErrorBody("INTERNAL_ERROR", "Blast radius computation failed"));
    }

    // Minimal error payload — avoids leaking stack traces to clients
    record ErrorBody(String code, String message) {}
}
