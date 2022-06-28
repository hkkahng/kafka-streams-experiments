package com.fico.kafkastreams.showcase.utils;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public abstract class StreamConfigurationUtils {

    public static Properties streamProperties() {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-exp");
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());
        streamProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamProperties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        streamProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);
        streamProperties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "TRACE");
        long cacheMaxBytes = 8 * 1024;
        streamProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheMaxBytes);
        return streamProperties;
    }

}
