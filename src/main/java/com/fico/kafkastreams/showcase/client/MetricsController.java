package com.fico.kafkastreams.showcase.client;

import com.fico.kafkastreams.showcase.metrics.DefaultMetricsService;
import com.fico.kafkastreams.showcase.metrics.MetricsService;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping(value = "/metrics")
public class MetricsController {

    private final DefaultMetricsService metricsService;

    @GetMapping(value = "/latency")
    public ResponseEntity<Map<String, String>> getLatencyHistogram() {
        Map<String, String> histogramValues = new HashMap<>();

        DistributionSummary latencyDistributionSummary = metricsService.getMeterRegistry().summary(MetricsService.Histograms.PROCESSING_LATENCY.getKey());

        HistogramSnapshot latencySnapshot = latencyDistributionSummary.takeSnapshot();

        histogramValues.put("count", Long.toString(latencySnapshot.count()));
        histogramValues.put("mean", Double.toString(latencySnapshot.mean()));
        for (ValueAtPercentile valueAtPercentile : latencySnapshot.percentileValues()) {
            histogramValues.put(Double.toString(valueAtPercentile.percentile()), Double.toString(valueAtPercentile.value()));
        }

        return ResponseEntity.ok(histogramValues);
    }
}
