package com.fico.kafkastreams.showcase.streams;

import com.fico.kafkastreams.showcase.StreamsExpProperties;
import com.fico.kafkastreams.showcase.metrics.DefaultMetricsService;
import com.fico.kafkastreams.showcase.metrics.KafkaStreamsMetricsBinderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Component
public class PrimitiveProcessingService {

    private final StreamsExpProperties streamsExpProperties;
    private final Properties streamProperties;
    private final KafkaStreamsMetricsBinderService kafkaStreamsMetricsBinderService;
    private final DefaultMetricsService metricsService;

    private KafkaStreams streams;

    @EventListener(ApplicationStartedEvent.class)
    public void startProcessingTopology() {
        log.info("Should start kafka streams application here!");
        log.info("primitive record source topic: {}", streamsExpProperties.getPrimitiveRecordSourceTopic());
        log.info("primitive record sink topic: {}", streamsExpProperties.getPrimitiveRecordSinkTopic());
        log.info("simple record source topic: {}", streamsExpProperties.getSimpleRecordSourceTopic());
        log.info("simple record sink topic: {}", streamsExpProperties.getSimpleRecordSinkTopic());
        log.info("single partition source topic: {}", streamsExpProperties.getSinglePartitionSourceTopic());
        log.info("single partition sink topic: {}", streamsExpProperties.getSinglePartitionSinkTopic());

        PrimitiveProcessingTopologyBuilder primitiveProcessingTopologyBuilder = new PrimitiveProcessingTopologyBuilder(streamsExpProperties, metricsService);

        streams = new KafkaStreams(primitiveProcessingTopologyBuilder.noOpWithRepartitionsTopology(), streamProperties);
        kafkaStreamsMetricsBinderService.setKafkaStreams(streams);
        streams.setStateListener(kafkaStreamsMetricsBinderService);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public List<String> getStreamsMetricsNames() {
        List<String> metricsNameList = streams.metrics().keySet()
                .stream()
                .map(MetricName::toString)
                .collect(Collectors.toList());

        return metricsNameList;
    }
}
