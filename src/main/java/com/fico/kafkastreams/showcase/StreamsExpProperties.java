package com.fico.kafkastreams.showcase;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("streamsexp")
@Data
public class StreamsExpProperties {

    private String primitiveRecordSourceTopic;
    private String primitiveRecordSinkTopic;
    private String simpleRecordSourceTopic;
    private String simpleRecordSinkTopic;
    private String singlePartitionSourceTopic;
    private String singlePartitionSinkTopic;
}
