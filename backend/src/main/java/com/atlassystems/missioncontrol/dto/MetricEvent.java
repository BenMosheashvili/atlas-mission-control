package com.atlassystems.missioncontrol.dto;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
@JsonIgnoreProperties(ignoreUnknown = true)
public record MetricEvent(String nodeId, String metricType, double value,
    double cpu, double mem, double latencyP99, int rps,
    double health, double traffic, long timestamp) {}
