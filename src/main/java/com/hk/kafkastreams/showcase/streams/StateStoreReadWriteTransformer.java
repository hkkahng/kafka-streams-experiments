package com.hk.kafkastreams.showcase.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Map;
import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
public class StateStoreReadWriteTransformer implements ValueTransformerWithKey<String, Double, Double> {

    private final String stateStoreName;
    private final TimeWindows windows;

    private ProcessorContext context;
    private long previousEventTime = 0;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public Double transform(String readOnlyKey, Double value) {
        long eventTime = context.timestamp();
        log.info("eventTime [{}]", eventTime);
        TimestampedWindowStore<String, Double> store = context.getStateStore(stateStoreName);

        Map<Long, TimeWindow> activeWindows = windows.windowsFor(eventTime);
        log.info("number of active windows for eventTime [{}]: [{}]", eventTime, activeWindows.size());

        long windowStart;
        long windowEnd;
        for (TimeWindow timeWindow : activeWindows.values()) {
            windowStart = timeWindow.start();
            windowEnd = timeWindow.end();
            log.info("current window start [{}] end [{}]", windowStart, windowEnd);

            log.info("looking in state store for a previous value for key [{}], windowStart[{}]", readOnlyKey, windowStart);
            ValueAndTimestamp<Double> storeValue = store.fetch(readOnlyKey, windowStart);
            if (Objects.nonNull(storeValue)) {
                log.info("entry-value [{}], timestamp [{}] found for key [{}] in windowed state store", storeValue.value(), storeValue.timestamp(), readOnlyKey);
                store.put(readOnlyKey, ValueAndTimestamp.make(value, eventTime), windowStart);
            }
            else {
                log.info("no entry found, adding value to state store with key [{}]. eventTime [{}], and windowStart [{}]", readOnlyKey, eventTime, windowStart);
                store.put(readOnlyKey, ValueAndTimestamp.make(value, eventTime), windowStart);
            }
        }
        previousEventTime = eventTime;

        return value;
    }

    @Override
    public void close() {

    }
}
