package com.fico.kafkastreams.showcase.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;

public abstract class StreamConfigurationUtils {

    public static final String PROCESSING_START_HEADER = "processing-start";

    public static Properties streamProperties() {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-exp");
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());
        //streamProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamProperties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        //streamProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100); // default is 30000 ms but is set to 100 ms if EXACTLY_ONCE processing
        //streamProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // custom value from FX, default is 10485760 bytes
        //streamProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3); // default is 1

        //streamProperties.put(producerPrefix(ProducerConfig.RETRIES_CONFIG), kafkaProperties.getTopicRetry()); // default is Integer.MAX
        //streamProperties.put(producerPrefix(ProducerConfig.LINGER_MS_CONFIG), kafkaProperties.getTopicLingerMs()); // default is 0 ms
        //streamProperties.put(producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), kafkaProperties.getTopicBatchSize()); // default is 16384 bytes
        //streamProperties.put(producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), kafkaProperties.getMaxInFlightConnectionPerRequest()); // default is 5
        //streamProperties.put(producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), kafkaProperties.getCompression()); // default at producer level is none
        //streamProperties.put(producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), kafkaProperties.getMaxRequestSizeBytes()); // default is 1048576 bytes
        //streamProperties.put(producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), kafkaProperties.getMaxBlockMsConfig()); // default is 60000 ms
        //streamProperties.put(producerPrefix(ProducerConfig.ACKS_CONFIG), kafkaProperties.getAcksConfig()); // default is ALL (-1) for EXACTLY_ONCE processing
        //streamProperties.put(producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), kafkaProperties.getTransactionTimeoutConfig()); // default is 60000 ms, FX custom is 100000

        //streamProperties.put(consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 250); // default is 500, FX custom value is 250
        //streamProperties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), kafkaProperties.getOffsetReset()); // default is latest
        //streamProperties.put(consumerPrefix(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG), kafkaProperties.getMaxPartitionFetchBytes()); // default is 1048576, FX custom is 1572864

        streamProperties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "TRACE");
        streamProperties.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 30000);

        return streamProperties;
    }


}
