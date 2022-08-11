package com.hk.kafkastreams.showcase.metrics;

import io.micrometer.core.instrument.Tag;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Map;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

public class DoubleValueMetricProvider implements ToDoubleFunction<String> {

    private KafkaStreams kafkaStreams;

    public DoubleValueMetricProvider(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @Override
    public double applyAsDouble(String value) {
        return 0;
    }

    public void foo (Map<String, String> rawTags) {

        Iterable<Tag> tags =
        rawTags.entrySet()
                .stream()
                .map(entry -> Tag.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

    }
}
