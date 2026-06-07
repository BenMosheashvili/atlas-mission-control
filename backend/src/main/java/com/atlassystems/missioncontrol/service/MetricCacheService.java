package com.atlassystems.missioncontrol.service;
import com.atlassystems.missioncontrol.dto.MetricEvent;
import lombok.RequiredArgsConstructor; import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration; import java.util.concurrent.CompletableFuture;
@Slf4j @Service @RequiredArgsConstructor
public class MetricCacheService {
    private final ReactiveRedisTemplate<String,Object> redisTemplate;
    public CompletableFuture<Void> updateMetric(MetricEvent e) {
        return redisTemplate.opsForValue()
            .set("infra:metric:"+e.nodeId()+":"+e.metricType(), e.value(), Duration.ofMinutes(5))
            .toFuture().thenApply(r->null);
    }
}
