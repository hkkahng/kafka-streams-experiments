package com.hk.kafkastreams.showcase.streams;

import com.hk.kafkastreams.showcase.StreamsExpProperties;
import com.hk.kafkastreams.showcase.metrics.DefaultMetricsService;
import com.hk.kafkastreams.showcase.utils.StreamConfigurationUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

@Slf4j
@RequiredArgsConstructor
public class PrimitiveProcessingTopologyBuilder {

    private final StreamsExpProperties streamsExpProperties;
    private final DefaultMetricsService metricsService;

    private StreamsBuilder streamsBuilder;
    private StateStoreUtils stateStoreUtils;


    public Topology sumAggregationTopology() {
        streamsBuilder = new StreamsBuilder();
        logSourceAndSinkTopicInfo();
        KStream<String, Double> recordStream = streamsBuilder.stream(streamsExpProperties.getPrimitiveRecordSourceTopic());

        KTable<String, Double> sumAggregationTable =
                recordStream
                        .peek((key, value) -> log.info("record: key [{}] value [{}]", key, value), Named.as("peek-output-record"))
                        .groupByKey(Grouped.as("group-on-key"))
                        .reduce((aggValue, newValue) -> aggValue + newValue, Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("aggregation-sum-store"));

        log.info("sum aggregation KTable queryable store name: [{}]", sumAggregationTable.queryableStoreName());

        sumAggregationTable
                .toStream(Named.as("sum-result-stream"))
                .peek((key, value) -> log.info("sum update: key[{}] value [{}]", key, value))
                .to(streamsExpProperties.getPrimitiveRecordSinkTopic());

        KTable<String, Long> countAggregationTable =
                recordStream
                        .peek((key, value) -> log.info("record: key [{}] value [{}] (for counting)", key, value), Named.as("peek-count-record"))
                        .groupByKey(Grouped.as("group-on-key-for-count"))
                        .count(Named.as("record-count"), Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("record-count-store"));


        Topology topology = streamsBuilder.build(StreamConfigurationUtils.streamProperties());
        log.info(topology.describe().toString());

        return topology;
    }

    public Topology noOpTopology() {
        streamsBuilder = new StreamsBuilder();
        logSourceAndSinkTopicInfo();

        KStream<String, Double> recordStream = streamsBuilder.stream(streamsExpProperties.getPrimitiveRecordSourceTopic());

        recordStream
                .peek((key, value) -> log.info("no-op-1: key [{}], value [{}]", key, value), Named.as("no-op-node-1"))
                .to(streamsExpProperties.getPrimitiveRecordSinkTopic());

        Topology topology = streamsBuilder.build(StreamConfigurationUtils.streamProperties());
        log.info(topology.describe().toString());

        return topology;
    }

    public Topology noOpWithRepartitionsTopology() {
        streamsBuilder = new StreamsBuilder();
        stateStoreUtils = new StateStoreUtils();
        logSourceAndSinkTopicInfo();

        streamsBuilder.addStateStore(stateStoreUtils.getWindowedStateStore(StateStoreUtils.STATE_STORE_1_NAME));

        KStream<String, Double> inputStream = streamsBuilder.stream(streamsExpProperties.getPrimitiveRecordSourceTopic());

        inputStream
                .transformValues(PrimitiveRecordMarkStartTimestamp::new)
                .peek((key, value) -> {}, Named.as("no-op-node-1"))
                .repartition()
                .peek((key, value) -> {}, Named.as("no-op-node-2"))
                .transformValues(() -> new StateStoreReadWriteTransformer(StateStoreUtils.STATE_STORE_1_NAME, WindowUtils.getDefaultTimeWindow()), StateStoreUtils.STATE_STORE_1_NAME)
                .repartition()
                .peek((key, value) -> {}, Named.as("no-op-node-3"))
                .transformValues(() -> new PrimitiveRecordMarkEndTimestamp(metricsService))
                .to(streamsExpProperties.getPrimitiveRecordSinkTopic());

        Topology topology = streamsBuilder.build();
        log.info(topology.describe().toString());

        return topology;
    }

    public Topology noOpWithDirectRepartitionsTopology() {
        streamsBuilder = new StreamsBuilder();
        logSourceAndSinkTopicInfo();

        KStream<String, Double> inputStream = streamsBuilder.stream(streamsExpProperties.getPrimitiveRecordSourceTopic());

        inputStream
                .transformValues(PrimitiveRecordMarkStartTimestamp::new)
                .peek((key, value) -> {}, Named.as("no-op-node-1"))
                .to("rp1");

        streamsBuilder.stream("rp1")
                .peek((key, value) -> {}, Named.as("no-op-node-2"))
                .to("rp2");

        KStream<String, Double> finalSegment = streamsBuilder.stream("rp2");
        finalSegment
                .peek((key, value) -> {}, Named.as("no-op-node-3"))
                .transformValues(() -> new PrimitiveRecordMarkEndTimestamp(metricsService))
                .to(streamsExpProperties.getPrimitiveRecordSinkTopic());

        Topology topology = streamsBuilder.build();
        log.info(topology.describe().toString());

        return topology;
    }

    public Topology noOpTopologyWithSimpleValueTransformer() {
        streamsBuilder = new StreamsBuilder();

        logSourceAndSinkTopicInfo();

        KStream<String, Double> recordStream = streamsBuilder.stream(streamsExpProperties.getPrimitiveRecordSourceTopic());

        recordStream
                .transformValues(() -> new PrimitiveRecordValueTransformer(), Named.as("simple-value-transformer"))
                .to(streamsExpProperties.getPrimitiveRecordSinkTopic());

        Topology topology = streamsBuilder.build(StreamConfigurationUtils.streamProperties());
        log.info(topology.describe().toString());

        return topology;
    }

    public Topology streamToTableJoins() {
        streamsBuilder = new StreamsBuilder();
        logSourceAndSinkTopicInfo();
        KStream<String, Double> recordStream = streamsBuilder.stream(streamsExpProperties.getPrimitiveRecordSourceTopic());

        KTable<String, Double> sumAggregationTable =
                recordStream
                        .peek((key, value) -> log.info("record: key [{}] value [{}]", key, value), Named.as("peek-output-record"))
                        .groupByKey(Grouped.as("group-on-key"))
                        .reduce((aggValue, newValue) -> aggValue + newValue, Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("aggregation-sum-store"));

        log.info("sum aggregation KTable queryable store name: [{}]", sumAggregationTable.queryableStoreName());

        KStream<String, Double> changedKeyStream = recordStream.selectKey(new PrimitiveRecordArbitraryKeyValueMapper(), Named.as("change-key"));

        changedKeyStream.leftJoin(sumAggregationTable,
                new PrimitiveRecordValueJoiner(),
                Joined.<String, Double, Double>keySerde(Serdes.String()).withValueSerde(Serdes.Double()).withOtherValueSerde(Serdes.Double()))
                .peek((key, value) -> log.info("after join, key [{}] value [{}]", key, value))
                .to(streamsExpProperties.getPrimitiveRecordSinkTopic());

        Topology topology = streamsBuilder.build(StreamConfigurationUtils.streamProperties());
        log.info(topology.describe().toString());

        return topology;
    }

    public Topology windowedCountAggregations() {
        streamsBuilder = new StreamsBuilder();
        logSourceAndSinkTopicInfo();
        KStream<String, Double> recordStream = streamsBuilder.stream(streamsExpProperties.getPrimitiveRecordSourceTopic());

        KGroupedStream<String, Double> groupedStream = recordStream.groupByKey(Grouped.as("group-on-key"));
        TimeWindowedKStream<String, Double> timeWindowedStream = groupedStream.windowedBy(WindowUtils.getDefaultTimeWindow());

        KTable<Windowed<String>, Long> countAggregationTable = timeWindowedStream
                .count(Named.as("windowed-record-count"), Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("windowed-record-count-store"));

        KStream<Windowed<String>, Long> windowedCountAggregationStream = countAggregationTable.toStream();
        windowedCountAggregationStream.peek((key, value) -> {
            log.info("window details: [{}], count: [{}]", key.window().toString(), value);
        });

        Topology topology = streamsBuilder.build(StreamConfigurationUtils.streamProperties());
        log.info(topology.describe().toString());

        return topology;
    }

    public Topology windowedSumAggregations() {
        streamsBuilder = new StreamsBuilder();
        logSourceAndSinkTopicInfo();
        KStream<String, Double> recordStream = streamsBuilder.stream(streamsExpProperties.getPrimitiveRecordSourceTopic());

        KGroupedStream<String, Double> groupedStream = recordStream.groupByKey(Grouped.as("group-on-key"));
        TimeWindowedKStream<String, Double> timeWindowedStream = groupedStream.windowedBy(WindowUtils.getDefaultTimeWindow());

        KTable<Windowed<String>, Double> countAggregationTable = timeWindowedStream
                .reduce((aggValue, newValue) -> aggValue + newValue, Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("windowed-record-sum-store"));

        KStream<Windowed<String>, Double> windowedCountAggregationStream = countAggregationTable.toStream();
        windowedCountAggregationStream.peek((key, value) -> {
            log.info("window details: [{}], count: [{}]", key.window().toString(), value);
        });

        Topology topology = streamsBuilder.build(StreamConfigurationUtils.streamProperties());
        log.info(topology.describe().toString());

        return topology;
    }

    public Topology slidingWindowCountAggregation() {
        streamsBuilder = new StreamsBuilder();
        logSourceAndSinkTopicInfo();;
        KStream<String, Double> recordStream = streamsBuilder.stream(streamsExpProperties.getPrimitiveRecordSourceTopic());

        KGroupedStream<String, Double> groupedStream = recordStream.groupByKey(Grouped.as("group-on-key"));
        TimeWindowedKStream<String, Double> timeWindowedStream = groupedStream.windowedBy(WindowUtils.getDefaultSlidingWindow());

        KTable<Windowed<String>, Long> slidingWindowCountAggregations =
                timeWindowedStream
                        .count(Named.as("sliding-window-count"), Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("sliding-window-count-store"));

        KStream<Windowed<String>, Long> slidingWindowCountStream = slidingWindowCountAggregations.toStream();
        slidingWindowCountStream.peek((key, value) -> log.info("window details: [{}], count [{}]", key.window().toString(), value));

        Topology topology = streamsBuilder.build(StreamConfigurationUtils.streamProperties());
        log.info(topology.describe().toString());

        return topology;
    }

    private void logSourceAndSinkTopicInfo() {
        log.info("source topic: {}", streamsExpProperties.getPrimitiveRecordSourceTopic());
        log.info("sink topic: {}", streamsExpProperties.getPrimitiveRecordSinkTopic());
    }
}
