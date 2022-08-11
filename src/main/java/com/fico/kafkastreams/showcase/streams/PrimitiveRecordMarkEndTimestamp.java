package com.fico.kafkastreams.showcase.streams;

import com.fico.kafkastreams.showcase.metrics.DefaultMetricsService;
import com.fico.kafkastreams.showcase.utils.StreamConfigurationUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.ByteBuffer;

@Slf4j
public class PrimitiveRecordMarkEndTimestamp implements ValueTransformer<Double, Double> {

    private final DefaultMetricsService metricsService;
    private ProcessorContext context;

    public PrimitiveRecordMarkEndTimestamp(DefaultMetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public Double transform(Double value) {
        for (Header header: context.headers().headers(StreamConfigurationUtils.PROCESSING_START_HEADER)) {
            long startTimestamp = ByteBuffer.wrap(header.value()).getLong();
            long latency = System.currentTimeMillis() - startTimestamp;
            //log.info("extracting {} [{}]", header.key(), ByteBuffer.wrap(header.value()).getLong());
            log.info("total processing time of record: [{}] ms", latency);
            metricsService.recordLatency(latency);
        }

        return value;
    }

    @Override
    public void close() {

    }
}
