package com.fico.kafkastreams.showcase.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;

@Slf4j
public class PrimitiveRecordValueJoiner implements ValueJoiner<Double, Double, Double> {

    @Override
    public Double apply(Double leftValue, Double rightValue) {
        log.info("Left value [{}], right value [{}]", leftValue, rightValue);
        return leftValue + rightValue;
    }
}
