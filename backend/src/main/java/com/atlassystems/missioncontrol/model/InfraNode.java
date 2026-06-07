package com.atlassystems.missioncontrol.model;
import lombok.Builder; import lombok.Value; import java.util.List;
@Value @Builder
public class InfraNode {
    String nodeId, name, group, environment;
    double healthScore;
    List<String> dependencies;
}
