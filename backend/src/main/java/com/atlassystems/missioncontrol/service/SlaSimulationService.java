package com.atlassystems.missioncontrol.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * SlaSimulationService
 *
 * Simulates pulling historical MTTR data from an external SLA system
 * (e.g. PagerDuty, ServiceNow, or an internal incident database).
 *
 * In production: replace computeMttr() body with an HTTP call to the
 * SLA API, passing nodeGroup + severity as query params.
 * The interface (input/output contract) stays identical.
 *
 * ── MTTR Logic ───────────────────────────────────────────────────────────────
 *
 * Base MTTR is keyed by node GROUP (architectural role), not by node ID.
 * Rationale: MTTR is a property of the failure MODE, not the instance.
 * A "db-cluster-replica" fails the same way as "db-cluster-primary".
 *
 * Modifier applied on top of base:
 *   CRITICAL_PATH_BONUS  +10m   — node on critical path → higher blast severity
 *   CASCADE_BONUS        +5m/hop— every extra hop in cascade adds remediation time
 *
 * Base MTTR table (minutes) — sourced from industry SRE benchmarks:
 *   data     (DB, object store, cache)  →  45m  (data corruption risk → careful)
 *   security (auth vault)               →  35m  (security review required)
 *   compute  (k8s, vm pools)            →  25m  (restart + health-check cycle)
 *   infra    (message bus, queues)      →  20m  (drain + replay required)
 *   edge     (API gateway, CDN)         →  12m  (stateless → fast rollback)
 *   observe  (logging, metrics)         →   8m  (non-critical path → quick swap)
 *   unknown  (fallback)                 →  30m
 * ─────────────────────────────────────────────────────────────────────────────
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SlaSimulationService {

    // ── Base MTTR by node group ───────────────────────────────────────────────

    private static final Map<String, Integer> BASE_MTTR_BY_GROUP = Map.of(
        "data",     45,
        "security", 35,
        "compute",  25,
        "infra",    20,
        "edge",     12,
        "observe",   8
    );

    private static final int FALLBACK_MTTR         = 30;
    private static final int CRITICAL_PATH_BONUS   = 10;   // minutes
    private static final int CASCADE_HOP_PENALTY   =  5;   // minutes per extra hop

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Estimate MTTR for a failing node.
     *
     * @param nodeGroup       Group property of the origin node (from Neo4j).
     * @param isOnCritPath    Whether this node sits on the Dijkstra critical path.
     * @param cascadeDepth    Number of downstream hops affected (blast depth).
     * @return                Estimated time-to-recover in minutes.
     */
    public int computeMttr(String nodeGroup, boolean isOnCritPath, int cascadeDepth) {
        int baseMttr = BASE_MTTR_BY_GROUP.getOrDefault(
            normalizeGroup(nodeGroup),
            FALLBACK_MTTR
        );

        int critBonus    = isOnCritPath ? CRITICAL_PATH_BONUS : 0;
        // Cascade beyond 2 hops adds remediation complexity; cap at 5 hops
        int cascadeBonus = Math.min(cascadeDepth, 5) * CASCADE_HOP_PENALTY;

        int total = baseMttr + critBonus + cascadeBonus;

        log.info("[SlaService] MTTR: group={} base={}m critBonus={}m cascadeBonus={}m → total={}m",
                 nodeGroup, baseMttr, critBonus, cascadeBonus, total);

        return total;
    }

    /**
     * Compute a 0–100 impact score from blast radius metrics.
     *
     * Formula:
     *   latencyFactor  = min(totalLatencyMs / 500, 1.0)  → higher latency = higher impact
     *   blastFactor    = min(totalAffectedNodes / 10, 1.0)
     *   weightedScore  = (latencyFactor × 0.6 + blastFactor × 0.4) × 100
     *   capped at 99 — 100 reserved for "total infrastructure loss" alerts
     */
    public int computeImpactScore(double totalLatencyMs, int totalAffectedNodes) {
        double latencyFactor = Math.min(totalLatencyMs / 500.0, 1.0);
        double blastFactor   = Math.min(totalAffectedNodes / 10.0, 1.0);
        int score = (int) Math.round((latencyFactor * 0.6 + blastFactor * 0.4) * 100);
        return Math.min(score, 99);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Normalize free-text group values to canonical keys. */
    private String normalizeGroup(String raw) {
        if (raw == null) return "unknown";
        String lower = raw.toLowerCase().trim();
        // Handle compound labels like "data-primary", "edge-cdn", etc.
        for (String key : BASE_MTTR_BY_GROUP.keySet()) {
            if (lower.startsWith(key)) return key;
        }
        return lower;
    }
}
