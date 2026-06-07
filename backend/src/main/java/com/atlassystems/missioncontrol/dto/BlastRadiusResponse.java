package com.atlassystems.missioncontrol.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Value;

import java.util.List;

/**
 * Exact JSON contract for GET /api/v1/infra/blast-radius/{originNodeId}
 *
 * Immutable via @Value + @Builder (Lombok).
 * Nulls excluded from serialization — secondaryBlastNodes and upstreamCallers
 * may be empty lists but never null.
 */
@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BlastRadiusResponse {

    String originId;
    int    impactScore;           // 0–100: derived from criticalPath total latency + blast depth
    int    estimatedMttrMinutes;  // from SlaSimulationService

    CriticalPath criticalPath;
    List<String> secondaryBlastNodes;
    List<String> upstreamCallers;

    @Value
    @Builder
    public static class CriticalPath {
        List<String> nodes;
        double       totalLatencyMs;
    }
}
