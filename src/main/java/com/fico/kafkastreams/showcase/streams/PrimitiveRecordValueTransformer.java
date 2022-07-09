package com.fico.kafkastreams.showcase.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class PrimitiveRecordValueTransformer implements ValueTransformerWithKey<String, Double, Double> {

    @Override
    public void init(ProcessorContext context) {
        // noop
    }

    @Override
    public Double transform(String readOnlyKey, Double value) {
        return value * 2;
    }

    @Override
    public void close() {
        // noop
    }
}
