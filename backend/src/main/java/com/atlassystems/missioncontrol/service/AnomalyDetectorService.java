package com.atlassystems.missioncontrol.service;

import com.atlassystems.missioncontrol.dto.MetricEvent;
import com.atlassystems.missioncontrol.kafka.AnomalyResult;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.DoubleSummaryStatistics;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AnomalyDetectorService
 *
 * ── Design decisions ──────────────────────────────────────────────────────────
 *
 * 1. IN-MEMORY SLIDING WINDOW per (serviceId, metricType)
 *    We keep a bounded Deque<Double> per metric key.  The key is the composite
 *    string "{serviceId}:{metricType}" so CPU and memory of the same service
 *    maintain separate windows.  Window size is configurable per environment
 *    (default 60 samples ≈ 1 min at 1-second resolution).
 *
 *    We use ArrayDeque (not LinkedList) for cache-friendly sequential iteration
 *    during stats calculation — critical when this runs on the hot evaluation path.
 *
 * 2. Z-SCORE algorithm
 *    Z = (x - μ) / σ   where μ and σ are computed from the current window.
 *    |Z| > threshold (default 3.0) → anomaly flagged.
 *    We require a minimum window fill (MIN_SAMPLES) before evaluating to avoid
 *    false positives during cold-start.
 *
 * 3. THREAD SAFETY
 *    ConcurrentHashMap provides safe concurrent access to the window map.
 *    computeIfAbsent is atomic for window creation.
 *    Per-window mutation (addLast / pollFirst) is guarded by synchronized on the
 *    specific Deque instance — fine-grained locking, not a global lock.
 *    This allows different metric keys to be evaluated truly concurrently.
 *
 * 4. EVICTION / MEMORY BOUND
 *    A scheduled task (@Scheduled in AnomalyCleanupTask) removes windows for
 *    serviceIds that haven't emitted a metric in > EVICTION_TTL (default 5 min).
 *    This prevents unbounded growth if services are deregistered.
 *    We track lastSeen per key in a parallel ConcurrentHashMap<String, Long>.
 *
 * 5. POPULATION STANDARD DEVIATION
 *    We use population std dev (divide by N) rather than sample std dev (N-1)
 *    because we treat the window as the full observed population for this interval,
 *    not a sample from a larger distribution.  For N=60 the difference is negligible
 *    but the intent is cleaner.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 */
@Slf4j
@Service
public class AnomalyDetectorService {

    // ── Configuration ─────────────────────────────────────────────────────────

    /** Number of samples in the sliding window. */
    @Value("${anomaly.window.size:60}")
    private int windowSize;

    /** Z-score magnitude above which we flag an anomaly. */
    @Value("${anomaly.zscore.threshold:3.0}")
    private double zScoreThreshold;

    /** Minimum samples required before we start evaluating. */
    @Value("${anomaly.min.samples:10}")
    private int minSamples;

    /** Milliseconds of inactivity before a window is eligible for eviction. */
    @Value("${anomaly.eviction.ttl.ms:300000}")
    private long evictionTtlMs;

    // ── State ─────────────────────────────────────────────────────────────────

    /**
     * Map from composite key "{serviceId}:{metricType}" → sliding window of values.
     *
     * <p>Package-private for AnomalyCleanupTask eviction.
     */
    final ConcurrentHashMap<String, Deque<Double>> windows  = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Long>          lastSeen = new ConcurrentHashMap<>();

    private final MeterRegistry meterRegistry;

    public AnomalyDetectorService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Evaluate a metric event against the sliding window for its service+type.
     *
     * @param event  Incoming metric (serviceId, metricType, value, timestamp).
     * @return       AnomalyResult carrying whether an anomaly was detected and
     *               the computed Z-score.
     */
    public AnomalyResult evaluate(MetricEvent event) {
        String key   = windowKey(event.nodeId(), event.metricType());
        double value = event.value();

        // 1. Update the sliding window (thread-safe via per-Deque sync).
        addSample(key, value);

        // 2. Compute Z-score against the current window.
        ZScoreResult zResult = computeZScore(key, value);

        // 3. Guard: not enough data yet.
        if (!zResult.sufficient()) {
            return AnomalyResult.none();
        }

        boolean isAnomaly = Math.abs(zResult.z()) > zScoreThreshold;

        if (isAnomaly) {
            meterRegistry.counter("anomaly.detected",
                "service", event.nodeId(),
                "metric",  event.metricType()
            ).increment();

            log.warn("[AnomalyDetector] Anomaly: service={} metric={} value={} z={}",
                     event.nodeId(), event.metricType(), value, String.format("%.2f", zResult.z()));
        }

        return AnomalyResult.of(
            isAnomaly,
            zResult.z(),
            zResult.mean(),
            zResult.stdDev(),
            value
        );
    }

    /**
     * Return a snapshot of the current window for a metric key.
     * Used by the API layer to expose historical window data to the frontend.
     */
    public double[] getWindowSnapshot(String serviceId, String metricType) {
        Deque<Double> window = windows.get(windowKey(serviceId, metricType));
        if (window == null) return new double[0];
        synchronized (window) {
            return window.stream().mapToDouble(Double::doubleValue).toArray();
        }
    }

    // ── Internal sliding window mechanics ────────────────────────────────────

    /**
     * Append a sample to the window, evicting the oldest if window is full.
     * Synchronized on the specific Deque to allow concurrent access to other keys.
     */
    private void addSample(String key, double value) {
        long now = System.currentTimeMillis();

        Deque<Double> window = windows.computeIfAbsent(key, k -> new ArrayDeque<>(windowSize + 1));
        lastSeen.put(key, now);

        synchronized (window) {
            window.addLast(value);
            if (window.size() > windowSize) {
                window.pollFirst(); // evict oldest
            }
        }
    }

    /**
     * Compute mean, population std dev, and Z-score of {@code value} relative
     * to the current window contents.
     *
     * <p>We take a snapshot of the window under lock, then compute outside the
     * lock to minimize contention.
     */
    private ZScoreResult computeZScore(String key, double value) {
        Deque<Double> window = windows.get(key);
        if (window == null) return ZScoreResult.insufficient();

        double[] snapshot;
        int size;
        synchronized (window) {
            size = window.size();
            if (size < minSamples) return ZScoreResult.insufficient();
            snapshot = window.stream().mapToDouble(Double::doubleValue).toArray();
        }

        // Mean
        double sum = 0;
        for (double v : snapshot) sum += v;
        double mean = sum / size;

        // Population variance → std dev
        double variance = 0;
        for (double v : snapshot) {
            double diff = v - mean;
            variance += diff * diff;
        }
        double stdDev = Math.sqrt(variance / size);

        // Z-score: if stdDev ≈ 0 (all values identical), treat as no anomaly.
        if (stdDev < 1e-9) return ZScoreResult.of(0.0, mean, stdDev, true);

        double z = (value - mean) / stdDev;
        return ZScoreResult.of(z, mean, stdDev, true);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String windowKey(String serviceId, String metricType) {
        return serviceId + ':' + metricType;
    }

    // ── Eviction (called by AnomalyCleanupTask @Scheduled every 60s) ─────────

    public void evictStaleWindows() {
        long threshold = System.currentTimeMillis() - evictionTtlMs;
        lastSeen.forEach((key, ts) -> {
            if (ts < threshold) {
                windows.remove(key);
                lastSeen.remove(key);
                log.debug("[AnomalyDetector] Evicted stale window for key={}", key);
            }
        });
    }

    // ── Inner records ─────────────────────────────────────────────────────────

    private record ZScoreResult(double z, double mean, double stdDev, boolean sufficient) {
        static ZScoreResult insufficient() { return new ZScoreResult(0, 0, 0, false); }
        static ZScoreResult of(double z, double mean, double stdDev, boolean sufficient) {
            return new ZScoreResult(z, mean, stdDev, sufficient);
        }
    }
}
