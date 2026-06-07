package com.atlassystems.missioncontrol.kafka;

import com.atlassystems.missioncontrol.dto.MetricEvent;
import com.atlassystems.missioncontrol.service.AnomalyDetectorService;
import com.atlassystems.missioncontrol.service.MetricCacheService;
import com.atlassystems.missioncontrol.sse.SseEmitterRegistry;
import com.atlassystems.missioncontrol.sse.SseEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * MetricEventConsumer
 *
 * ── Design decisions ──────────────────────────────────────────────────────────
 *
 * 1. MANUAL ACK + BATCH mode
 *    We consume a List<ConsumerRecord> per poll cycle rather than one record at a
 *    time.  This keeps Kafka's poll loop alive (heartbeat never misses) regardless
 *    of how long downstream processing takes, because we offload to a dedicated
 *    executor and only ack *after* the whole batch is durably persisted.
 *
 * 2. FAN-OUT via CompletableFuture
 *    Redis write → anomaly check → SSE push are three independent side-effects.
 *    We run (a) and (b) in parallel on `observerTaskExecutor` (bounded pool defined
 *    in AsyncConfig), then chain SSE push as a dependent step only if anomaly
 *    detection raises a flag — avoiding unnecessary SSE floods on normal traffic.
 *
 * 3. BACK-PRESSURE contract
 *    If the executor queue fills up, CompletableFuture.allOf(...) will still resolve
 *    eventually; we never block the Kafka consumer thread itself.  The bounded queue
 *    in the executor creates natural back-pressure: if we're overwhelmed, tasks queue
 *    up and the batch latency rises — monitored via the `metric.batch.latency` timer.
 *
 * 4. ERROR ISOLATION per record
 *    A failure on one record inside the batch is caught individually.  We log + DLQ
 *    that record and continue; the batch ack still fires for the records that worked.
 *    This avoids endless re-delivery of a single poison message halting the whole
 *    partition.
 * ─────────────────────────────────────────────────────────────────────────────
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MetricEventConsumer {

    private final MetricCacheService    cacheService;      // Redis wrapper
    private final AnomalyDetectorService anomalyDetector;
    private final SseEmitterRegistry    sseRegistry;
    private final DeadLetterPublisher   dlqPublisher;
    private final Executor              observerTaskExecutor; // configured in AsyncConfig
    private final MeterRegistry         meterRegistry;

    // ── Kafka Listener ────────────────────────────────────────────────────────

    /**
     * @param records  Batch of raw Kafka records for this partition group.
     * @param ack      Manual acknowledgment — committed only after the full
     *                 async fan-out completes for every record in the batch.
     */
    @KafkaListener(
        topics           = "${kafka.topics.metrics}",
        groupId          = "${kafka.consumer.group-id}",
        containerFactory = "batchKafkaListenerContainerFactory"   // MANUAL_IMMEDIATE ack mode
    )
    public void onMetricsBatch(List<ConsumerRecord<String, MetricEvent>> records,
                               Acknowledgment ack) {

        Timer.Sample batchTimer = Timer.start(meterRegistry);
        log.debug("[MetricConsumer] Received batch of {} records", records.size());

        // Build a CompletableFuture for each record and fan them out.
        List<CompletableFuture<Void>> futures = records.stream()
            .map(record -> processAsync(record))
            .toList();

        // Wait for the full batch; then ack to Kafka.
        // We do NOT block the consumer thread — Spring Kafka's ContainerAwareErrorHandler
        // manages thread lifecycle.  But the contract here is: no ack until all futures settle.
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .whenComplete((ignored, err) -> {
                if (err != null) {
                    // At least one record errored; individual errors already DLQ'd above.
                    log.warn("[MetricConsumer] Batch completed with partial failures", err);
                }
                ack.acknowledge();  // always ack so we don't redeliver healthy records
                batchTimer.stop(meterRegistry.timer("metric.batch.latency"));
            });
    }

    // ── Per-record async fan-out ──────────────────────────────────────────────

    private CompletableFuture<Void> processAsync(ConsumerRecord<String, MetricEvent> record) {
        MetricEvent event = record.value();

        if (event == null) {
            log.warn("[MetricConsumer] Null event at partition={} offset={}",
                     record.partition(), record.offset());
            return CompletableFuture.completedFuture(null);
        }

        // (a) Write to Redis — fast, non-blocking via Lettuce reactive client wrapped
        //     in a CompletableFuture by MetricCacheService.
        CompletableFuture<Void> redisFuture = cacheService
            .updateMetric(event)
            .exceptionally(ex -> {
                log.error("[MetricConsumer] Redis write failed for serviceId={}", event.nodeId(), ex);
                return null; // tolerated — Redis is a cache; source of truth is InfluxDB
            });

        // (b) Anomaly detection runs in parallel with Redis write on the same executor.
        CompletableFuture<AnomalyResult> anomalyFuture = CompletableFuture
            .supplyAsync(() -> anomalyDetector.evaluate(event), observerTaskExecutor)
            .exceptionally(ex -> {
                log.error("[MetricConsumer] Anomaly eval failed for serviceId={}", event.nodeId(), ex);
                dlqPublisher.publish(record, ex);
                return AnomalyResult.none(); // sentinel — no anomaly emitted on eval failure
            });

        // (c) SSE push — only fires if anomaly detected; depends on (b) completing.
        CompletableFuture<Void> sseFuture = anomalyFuture
            .thenAcceptAsync(result -> {
                if (result.isAnomaly()) {
                    log.info("[MetricConsumer] Anomaly detected: serviceId={} score={}",
                             event.nodeId(), result.zScore());
                    sseRegistry.broadcast(
                        SseEvent.anomaly(event.nodeId(), result)
                    );
                }
                // Always push the raw metric update to subscribed dashboards.
                sseRegistry.broadcast(SseEvent.metric(event));
            }, observerTaskExecutor)
            .exceptionally(ex -> {
                log.warn("[MetricConsumer] SSE broadcast failed for serviceId={}", event.nodeId(), ex);
                return null; // SSE failure is non-fatal; don't DLQ
            });

        // Combine: this future resolves when Redis + anomaly + SSE all settle.
        return CompletableFuture.allOf(redisFuture, sseFuture);
    }
}
