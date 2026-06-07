package com.atlassystems.missioncontrol.sse;
import com.atlassystems.missioncontrol.dto.MetricEvent;
import com.atlassystems.missioncontrol.kafka.AnomalyResult;
public record SseEvent(String type, String nodeId, Object payload) {
    public static SseEvent metric(MetricEvent e)                    { return new SseEvent("metric",e.nodeId(),e); }
    public static SseEvent anomaly(String id, AnomalyResult r)      { return new SseEvent("anomaly",id,r); }
}
