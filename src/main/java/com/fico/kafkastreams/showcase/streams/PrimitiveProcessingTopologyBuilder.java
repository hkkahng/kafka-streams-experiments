package com.fico.kafkastreams.showcase.streams;

import com.fico.kafkastreams.showcase.StreamsExpProperties;
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
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import static com.fico.kafkastreams.showcase.utils.StreamConfigurationUtils.streamProperties;

@Slf4j
@RequiredArgsConstructor
public class PrimitiveProcessingTopologyBuilder {

    private final StreamsExpProperties streamsExpProperties;
    private StreamsBuilder streamsBuilder;

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


        Topology topology = streamsBuilder.build(streamProperties());
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

        Topology topology = streamsBuilder.build(streamProperties());
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

        Topology topology = streamsBuilder.build(streamProperties());
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

        Topology topology = streamsBuilder.build(streamProperties());
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

        Topology topology = streamsBuilder.build(streamProperties());
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

        Topology topology = streamsBuilder.build(streamProperties());
        log.info(topology.describe().toString());

        return topology;
    }

    private void logSourceAndSinkTopicInfo() {
        log.info("source topic: {}", streamsExpProperties.getPrimitiveRecordSourceTopic());
        log.info("sink topic: {}", streamsExpProperties.getPrimitiveRecordSinkTopic());
    }
}