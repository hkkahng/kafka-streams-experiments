package com.hk.kafkastreams.showcase.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.KeyValueMapper;

@Slf4j
public class PrimitiveRecordArbitraryKeyValueMapper implements KeyValueMapper<String, Double, String> {

    @Override
    public String apply(String key, Double value) {
        return StringUtils.swapCase(key);
    }
}
