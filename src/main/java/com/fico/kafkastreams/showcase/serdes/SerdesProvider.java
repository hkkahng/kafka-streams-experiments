package com.fico.kafkastreams.showcase.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fico.kafkastreams.showcase.model.SimpleRecord;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

@RequiredArgsConstructor
public class SerdesProvider {

    private final ObjectMapper objectMapper;

    public Serde<SimpleRecord> simpleRecordSerde() {
        return Serdes.serdeFrom(SimpleJsonSerializer.of(objectMapper, SimpleRecord.class), SimpleJsonDeserializer.of(objectMapper, SimpleRecord.class));
    }
}
