package com.fico.kafkastreams.showcase.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.lang.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaStreamsMetricsBinderService implements MeterBinder, KafkaStreams.StateListener {

    public static final String E2E_LATENCY_AVG = "record-e2e-latency-avg";

    public static final String E2E_LATENCY_THREAD_ID_TAG = "thread-id";
    public static final String E2E_LATENCY_TASK_ID_TAG = "task-id";
    public static final String E2E_LATENCY_PROCESSOR_NODE_ID_TAG = "processor-node-id";

    private KafkaStreams kafkaStreams;
    private MeterRegistry meterRegistry;

    @Override
    public void bindTo(@NonNull MeterRegistry registry) {
        this.meterRegistry = registry;
    }

    @Override
    public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (newState == KafkaStreams.State.RUNNING) {
            kafkaStreams.metrics().entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().name().equalsIgnoreCase(E2E_LATENCY_AVG))
                    .forEach(entry -> {
                        StringBuilder metricName = new StringBuilder(entry.getKey().name())
                                .append("|")
                                .append(entry.getKey().tags().get(E2E_LATENCY_PROCESSOR_NODE_ID_TAG))
                                .append("|")
                                .append(entry.getKey().tags().get(E2E_LATENCY_TASK_ID_TAG));
                        log.info("adding [{}] as a gauge", metricName);
                        meterRegistry.gauge(metricName.toString(), (Double) entry.getValue().metricValue());
                    });
        }
    }

    public void setKafkaStreams(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    static List<Tag> generateStandardTags(Map<String, String> rawTags) {
        return rawTags.entrySet().stream()
                .map(rawTag -> Tag.of(rawTag.getKey(), rawTag.getValue()))
                .collect(Collectors.toList());
    }
}
