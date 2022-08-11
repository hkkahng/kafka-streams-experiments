package com.hk.kafkastreams.showcase.streams;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedWindowStore;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.state.Stores.persistentTimestampedWindowStore;
import static org.apache.kafka.streams.state.Stores.timestampedWindowStoreBuilder;

public class StateStoreUtils {

    public static final String STATE_STORE_1_NAME = "state-store-1";
    public static final String STATE_STORE_2_NAME = "state-store-2";
    public static final String STATE_STORE_3_NAME = "state-store-3";

    public StoreBuilder<TimestampedWindowStore<String, Double>> getWindowedStateStore(String windowedStateStoreName) {

        return timestampedWindowStoreBuilder(
                    persistentTimestampedWindowStore(
                            windowedStateStoreName,
                            Duration.ofMinutes(WindowUtils.DEFAULT_WINDOW_RETENTION_IN_MINUTES),
                            WindowUtils.DEFAULT_WINDOW_SIZE,
                            false),
                    Serdes.String(),
                    Serdes.Double())
                .withLoggingEnabled(stateStoreChangeLogConfig());
    }

    public Map<String, String> stateStoreChangeLogConfig() {
        Map<String, String> stateStoreChangeLogConfig = new HashMap<>();
        stateStoreChangeLogConfig.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(1).toMillis()));

        return stateStoreChangeLogConfig;
    }
}
