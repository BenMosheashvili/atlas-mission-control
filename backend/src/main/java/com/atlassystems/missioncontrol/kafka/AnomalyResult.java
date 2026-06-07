package com.atlassystems.missioncontrol.kafka;
public record AnomalyResult(boolean isAnomaly, double zScore,
    double mean, double stdDev, double observedValue) {
    public static AnomalyResult none()  { return new AnomalyResult(false,0,0,0,0); }
    public static AnomalyResult of(boolean a,double z,double m,double s,double v) { return new AnomalyResult(a,z,m,s,v); }
}
