package com.fico.kafkastreams.showcase.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class DefaultMetricsService implements MetricsService {

    private static final Map<String, Timer> TIMERS = new ConcurrentHashMap<>();
    private static final Map<String, DistributionSummary> HISTOGRAMS = new ConcurrentHashMap<>();

    private final MeterRegistry meterRegistry;

    @PostConstruct
    public void initMetrics() {
        log.info("initializing custom metrics in [{}]", meterRegistry.config().toString());

        TIMERS.put(Timers.E2E_LATENCY.getKey(), meterRegistry.timer(Timers.E2E_LATENCY.getKey()));

        HISTOGRAMS.put(Histograms.PROCESSING_LATENCY.getKey(), DistributionSummary
                .builder(Histograms.PROCESSING_LATENCY.getKey())
                .publishPercentiles(0.90, 0.95)
                .serviceLevelObjectives(90, 95)
                .publishPercentileHistogram()
                .register(meterRegistry));

        log.info("meters found:");
        for (Meter meter : meterRegistry.getMeters()) {
            log.info("{}", meter.getId().getName());
        }
    }

    @Override
    public Timer getTimer(String key) {
        if (TIMERS.get(key) == null) {
            TIMERS.put(key, meterRegistry.timer(key));
        }
        return TIMERS.get(key);
    }

    public Optional<DistributionSummary> getHistogram(String key) {
        Optional<DistributionSummary> distributionSummary = Optional.ofNullable(HISTOGRAMS.get(key));
        return distributionSummary;
    }

    @Override
    public void recordTime(String key, long amount, TimeUnit timeUnit) {

    }

    public void recordLatency(long latency) {
        meterRegistry.summary(Histograms.PROCESSING_LATENCY.getKey()).record(latency);
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }
}
