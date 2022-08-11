package com.fico.kafkastreams.showcase.streams;

import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

public final class WindowUtils {

    public static final int DEFAULT_WINDOW_SIZE_IN_MINUTES = 2;
    public static final int DEFAULT_WINDOW_ADVANCE_INTERVAL_IN_SECONDS = 30;
    public static final int DEFAULT_GRACE_PERIOD_IN_MINUTES = 5;
    public static final int DEFAULT_WINDOW_RETENTION_IN_MINUTES = 30;

    public static final Duration DEFAULT_WINDOW_SIZE = Duration.ofMinutes(DEFAULT_WINDOW_SIZE_IN_MINUTES);
    public static final Duration DEFAULT_WINDOW_ADVANCE_INTERVAL = Duration.ofSeconds(DEFAULT_WINDOW_ADVANCE_INTERVAL_IN_SECONDS);
    public static final Duration DEFAULT_GRACE_PERIOD = Duration.ofMinutes(DEFAULT_GRACE_PERIOD_IN_MINUTES);


    public static TimeWindows getDefaultTimeWindow() {
        return TimeWindows
                .ofSizeWithNoGrace(DEFAULT_WINDOW_SIZE);
    }

    public static TimeWindows getTimeWindow(int windowSizeMinutes, int windowAdvanceIntervalMinutes, int gracePeriodMinutes) {
        return TimeWindows
                .of(Duration.ofMinutes(windowSizeMinutes))
                .advanceBy(Duration.ofMinutes(windowAdvanceIntervalMinutes))
                .grace(Duration.ofMinutes(gracePeriodMinutes));
    }


    public static SlidingWindows getDefaultSlidingWindow() {
        return SlidingWindows.withTimeDifferenceAndGrace(Duration.ofMinutes(3), Duration.ofMinutes(10));
    }

    public static SlidingWindows getSlidingWindow(int timeDifferenceMinutes, int gracePeriodMinutes) {
        return SlidingWindows.withTimeDifferenceAndGrace(Duration.ofMinutes(timeDifferenceMinutes), Duration.ofMinutes(gracePeriodMinutes));
    }
}
