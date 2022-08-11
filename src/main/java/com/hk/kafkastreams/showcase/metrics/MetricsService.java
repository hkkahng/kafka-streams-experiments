package com.hk.kafkastreams.showcase.metrics;

import io.micrometer.core.instrument.Timer;

import java.util.concurrent.TimeUnit;

public interface MetricsService {

    String RECORD_PROCESSING_START_TIME = "processing_start_time";
    String RECORD_PROCESSING_END_TIME = "processing_end_time";

    enum Timers {
        E2E_LATENCY("e2e_latency");

        private final String key;

        Timers(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }

    enum Histograms {
        PROCESSING_LATENCY("processing_latency");

        private final String key;

        Histograms(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }

    Timer getTimer(String key);

    void recordTime(String key, long amount, TimeUnit timeUnit);
}
