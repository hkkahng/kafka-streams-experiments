package com.hk.kafkastreams.showcase.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class SimpleJsonDeserializer<T> implements Deserializer<T> {

    private final ObjectReader reader;

    public static <T>SimpleJsonDeserializer<T> of(final ObjectMapper objectMapper, final Class<T> type) {
        return new SimpleJsonDeserializer<>(objectMapper.readerFor(type));
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        try {
            return reader.readValue(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }
}