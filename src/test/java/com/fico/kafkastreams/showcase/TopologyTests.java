package com.fico.kafkastreams.showcase;

import com.fico.kafkastreams.showcase.streams.PrimitiveProcessingTopologyBuilder;
import com.fico.kafkastreams.showcase.utils.StreamConfigurationUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Map;

public class TopologyTests {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyTests.class);

    private static StreamsExpProperties streamsExpProperties;
    private static Serde<String> stringSerde;
    private static Serde<Double> doubleSerde;
    private static String singleTestKey;

    @BeforeAll
    static void beforeAll() {
        streamsExpProperties = new StreamsExpProperties();
        streamsExpProperties.setPrimitiveRecordSourceTopic("p-source");
        streamsExpProperties.setPrimitiveRecordSinkTopic("p-sink");

        stringSerde = new Serdes.StringSerde();
        doubleSerde = new Serdes.DoubleSerde();

        singleTestKey = "474064ae-d634-467c-a8a4-6e4fe92e36f0";
    }

    @Test
    void sumAggregations() {
        String testKey = "474064ae-d634-467c-a8a4-6e4fe92e36f0";
        PrimitiveProcessingTopologyBuilder primitiveProcessingTopologyBuilder = new PrimitiveProcessingTopologyBuilder(streamsExpProperties);

        try (TopologyTestDriver testDriver = new TopologyTestDriver(primitiveProcessingTopologyBuilder.sumAggregationTopology(), StreamConfigurationUtils.streamProperties())) {
            Map<String, StateStore> stateStores = testDriver.getAllStateStores();
            for (String stateStoreName : stateStores.keySet()) {
                LOG.info("key [{}] : state store value [{}]", stateStoreName, stateStores.get(stateStoreName).name());
            }

            TestInputTopic<String, Double> inputTopic = testDriver.createInputTopic(streamsExpProperties.getPrimitiveRecordSourceTopic(), stringSerde.serializer(), doubleSerde.serializer());
            TestOutputTopic<String, Double> outputTopic = testDriver.createOutputTopic(streamsExpProperties.getPrimitiveRecordSinkTopic(), stringSerde.deserializer(), doubleSerde.deserializer());

            for (int i = 0; i < 50; i++) {
                inputTopic.pipeInput(testKey, RandomUtils.nextDouble(0, 100));
            }

            if (!outputTopic.isEmpty()) {
                LOG.info("output topic size: {}", outputTopic.getQueueSize());
            }

            ReadOnlyKeyValueStore<String, Double> aggregationSumStore = testDriver.getKeyValueStore("aggregation-sum-store");
            KeyValueIterator<String, Double> keyAndValues = aggregationSumStore.all();
            while (keyAndValues.hasNext()) {
                KeyValue<String, Double> currentKeyAndValue = keyAndValues.next();
                LOG.info("key: {} | value: {}", currentKeyAndValue.key, currentKeyAndValue.value.toString());
            }
            keyAndValues.close();
            Double sum = aggregationSumStore.get(testKey);
            LOG.info("sum of records processed: {}", sum.toString());

            ReadOnlyKeyValueStore<String, Long> countStore = testDriver.getKeyValueStore("record-count-store");
            Long count = countStore.get(testKey);
            LOG.info("count from count-store [{}]", count);

            Assertions.assertEquals(50, count);
        }
    }

    @Test
    void windowedCounts() {
        PrimitiveProcessingTopologyBuilder primitiveProcessingTopologyBuilder = new PrimitiveProcessingTopologyBuilder(streamsExpProperties);
        Instant p001 = Instant.now();
        Instant p002 = p001.plusSeconds(65);
        Instant p003 = p002.plusSeconds(65);
        Instant p004 = p003.plusSeconds(65);
        Instant p005 = p004.plusSeconds(65);

        Instant p006 = p001.plusSeconds(10);
        Instant p007 = p002.plusSeconds(10);
        Instant p008 = p004.plusSeconds(10);

        LOG.info(String.valueOf(p001.toEpochMilli()));
        LOG.info(String.valueOf(p002.toEpochMilli()));
        LOG.info(String.valueOf(p003.toEpochMilli()));
        LOG.info(String.valueOf(p004.toEpochMilli()));
        LOG.info(String.valueOf(p005.toEpochMilli()));
        LOG.info(String.valueOf(p006.toEpochMilli()));
        LOG.info(String.valueOf(p007.toEpochMilli()));
        LOG.info(String.valueOf(p008.toEpochMilli()));


        try (TopologyTestDriver testDriver = new TopologyTestDriver(primitiveProcessingTopologyBuilder.windowedCountAggregations(), StreamConfigurationUtils.streamProperties())) {
            Map<String, StateStore> stateStores = testDriver.getAllStateStores();
            for (String stateStoreName : stateStores.keySet()) {
                LOG.info("key [{}] : state store value [{}]", stateStoreName, stateStores.get(stateStoreName).name());
            }

            TestInputTopic<String, Double> inputTopic =
                    testDriver.createInputTopic(streamsExpProperties.getPrimitiveRecordSourceTopic(), stringSerde.serializer(), doubleSerde.serializer());

            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p001.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p002.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p003.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p004.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p005.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p006.toEpochMilli());
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p007.toEpochMilli());
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p008.toEpochMilli());

            ReadOnlyWindowStore<Windowed<String>, Long> windowStore = testDriver.getWindowStore("windowed-record-count-store");
            KeyValueIterator<Windowed<Windowed<String>>, Long> windowStoreKeyValues = windowStore.all();
            while (windowStoreKeyValues.hasNext()) {
                KeyValue<Windowed<Windowed<String>>, Long> keyValue = windowStoreKeyValues.next();
                LOG.info("key: [{}], value: [{}]", keyValue.key.toString(), keyValue.value.toString());
            }
            windowStoreKeyValues.close();

            Assertions.assertTrue(true);
        }
    }

    @Test
    void slidingWindowedCounts() {
        PrimitiveProcessingTopologyBuilder primitiveProcessingTopologyBuilder = new PrimitiveProcessingTopologyBuilder(streamsExpProperties);
        Instant p001 = Instant.now();
        Instant p002 = p001.plusSeconds(61);
        Instant p003 = p002.plusSeconds(61);
        Instant p004 = p003.plusSeconds(61);
        Instant p005 = p004.plusSeconds(61);
        Instant p006 = p005.plusSeconds(61);
        Instant p007 = p006.plusSeconds(61);
        Instant p008 = p007.plusSeconds(61);

        LOG.info(String.valueOf(p001.toEpochMilli()));
        LOG.info(String.valueOf(p002.toEpochMilli()));
        LOG.info(String.valueOf(p003.toEpochMilli()));
        LOG.info(String.valueOf(p004.toEpochMilli()));
        LOG.info(String.valueOf(p005.toEpochMilli()));
        LOG.info(String.valueOf(p006.toEpochMilli()));
        LOG.info(String.valueOf(p007.toEpochMilli()));
        LOG.info(String.valueOf(p008.toEpochMilli()));


        try (TopologyTestDriver testDriver = new TopologyTestDriver(primitiveProcessingTopologyBuilder.slidingWindowCountAggregation(), StreamConfigurationUtils.streamProperties())) {
            Map<String, StateStore> stateStores = testDriver.getAllStateStores();
            for (String stateStoreName : stateStores.keySet()) {
                LOG.info("key [{}] : state store value [{}]", stateStoreName, stateStores.get(stateStoreName).name());
            }

            TestInputTopic<String, Double> inputTopic =
                    testDriver.createInputTopic(streamsExpProperties.getPrimitiveRecordSourceTopic(), stringSerde.serializer(), doubleSerde.serializer());

            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p001.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p002.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p003.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p004.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p005.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p006.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p007.toEpochMilli());
            inputTopic.advanceTime(Duration.ofSeconds(61));
            inputTopic.pipeInput(singleTestKey, RandomUtils.nextDouble(0.0, 100.0), p008.toEpochMilli());

            ReadOnlyWindowStore<Windowed<String>, Long> windowStore = testDriver.getWindowStore("sliding-window-count-store");
            KeyValueIterator<Windowed<Windowed<String>>, Long> windowStoreKeyValues = windowStore.all();
            while (windowStoreKeyValues.hasNext()) {
                KeyValue<Windowed<Windowed<String>>, Long> keyValue = windowStoreKeyValues.next();
                LOG.info("key: [{}], value: [{}]", keyValue.key.toString(), keyValue.value.toString());
            }
            windowStoreKeyValues.close();

            Assertions.assertTrue(true);
        }
    }
}
