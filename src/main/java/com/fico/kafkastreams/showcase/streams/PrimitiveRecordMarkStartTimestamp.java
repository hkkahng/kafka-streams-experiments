package com.fico.kafkastreams.showcase.streams;

import com.fico.kafkastreams.showcase.utils.StreamConfigurationUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.ByteBuffer;

@Slf4j
public class PrimitiveRecordMarkStartTimestamp implements ValueTransformer<Double, Double> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public Double transform(Double value) {
        log.info("### original record timestamp [{}]", context.timestamp());

        ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis());
        context.headers().add(StreamConfigurationUtils.PROCESSING_START_HEADER,
                ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array());

        //for (Header header: context.headers().headers("processing-start")) {
        //    log.info("{} [{}]", header.key(), ByteBuffer.wrap(header.value()).getLong());
        //}

        return value;
    }

    @Override
    public void close() {

    }
}
