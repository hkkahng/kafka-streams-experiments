package com.fico.kafkastreams.showcase.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class SimpleJsonSerializer<T> implements Serializer<T> {

    private final ObjectWriter writer;

    public static <T>SimpleJsonSerializer<T> of(final ObjectMapper objectMapper, final Class<T> ignoredType) {
        return new SimpleJsonSerializer<>(objectMapper.writer());
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        try {
            return writer.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }
}