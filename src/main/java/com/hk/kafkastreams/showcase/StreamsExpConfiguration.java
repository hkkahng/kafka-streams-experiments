package com.hk.kafkastreams.showcase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hk.kafkastreams.showcase.serdes.SerdesProvider;
import com.hk.kafkastreams.showcase.utils.JsonUtils;
import com.hk.kafkastreams.showcase.utils.UuidKeyUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Properties;
import java.util.concurrent.Executor;

import static org.apache.kafka.streams.StreamsConfig.producerPrefix;

@Configuration
@EnableAsync
public class StreamsExpConfiguration {

    @Bean
    public ObjectMapper objectMapper() {
        return JsonUtils.getObjectMapper();
    }

    @Bean
    public SerdesProvider serdesProvider() {
        return new SerdesProvider(objectMapper());
    }

    @Bean
    public StreamsExpProperties streamsExpProperties() {
        return new StreamsExpProperties();
    }

    @Bean
    public UuidKeyUtils uuidKeyUtils() {
        return new UuidKeyUtils();
    }

    @Bean
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(5);
        executor.setQueueCapacity(20);
        executor.setThreadNamePrefix("async-rest-");
        executor.initialize();
        return executor;
    }

    @Bean("streamProperties")
    @Profile("!ccloud")
    public Properties streamProperties() {
        Properties streamProperties = baseStreamProperties();
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-exp");
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        return streamProperties;
    }

    @Bean("streamProperties")
    @Profile("ccloud")
    public Properties cCloudStreamProperties() {
        Properties streamProperties = baseStreamProperties();
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-exp-ccloud");
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-pgq85.us-west-2.aws.confluent.cloud:9092");
        streamProperties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        streamProperties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        streamProperties.put(SaslConfigs.SASL_JAAS_CONFIG, "[put your confluent cloud credentials here]");

        return streamProperties;
    }

    public Properties baseStreamProperties() {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());
        streamProperties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        //streamProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        //streamProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100); // default is 30000 ms but is set to 100 ms if EXACTLY_ONCE processing
        //streamProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // custom value from FX, default is 10485760 bytes
        //streamProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 12); // default is 1

        //streamProperties.put(producerPrefix(ProducerConfig.RETRIES_CONFIG), kafkaProperties.getTopicRetry()); // default is Integer.MAX
        streamProperties.put(producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 5); // default is 100ms
        //streamProperties.put(producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), kafkaProperties.getTopicBatchSize()); // default is 16384 bytes
        //streamProperties.put(producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), kafkaProperties.getMaxInFlightConnectionPerRequest()); // default is 5
        //streamProperties.put(producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), kafkaProperties.getCompression()); // default at producer level is none
        //streamProperties.put(producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), kafkaProperties.getMaxRequestSizeBytes()); // default is 1048576 bytes
        //streamProperties.put(producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), kafkaProperties.getMaxBlockMsConfig()); // default is 60000 ms
        streamProperties.put(producerPrefix(ProducerConfig.ACKS_CONFIG), "1"); // default is ALL (-1) for EXACTLY_ONCE processing
        //streamProperties.put(producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), kafkaProperties.getTransactionTimeoutConfig()); // default is 60000 ms, FX custom is 100000

        //streamProperties.put(consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 250); // default is 500, FX custom value is 250
        //streamProperties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), kafkaProperties.getOffsetReset()); // default is latest
        //streamProperties.put(consumerPrefix(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG), kafkaProperties.getMaxPartitionFetchBytes()); // default is 1048576, FX custom is 1572864

        streamProperties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "TRACE");
        streamProperties.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 30000);
        streamProperties.put(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG, 2);
        return streamProperties;
    }
}
