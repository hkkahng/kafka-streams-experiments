package com.hk.kafkastreams.showcase.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Locale;
import java.util.Properties;

public abstract class ClientConfigurationUtils {

    private static final int CONSUMER_RANDOM_GROUP_ID_LENGTH = 8;

    public static Properties consumerProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, generateRandomConsumerGroupId());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT));

        return consumerProperties;
    }

    public static Properties consumerPropertiesWithMaxPollRecords(int maxPollRecords) {
        Properties consumerProperties = consumerProperties();
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(maxPollRecords));

        return  consumerProperties;
    }

    public static String generateRandomConsumerGroupId() {
        return RandomStringUtils.randomAlphanumeric(CONSUMER_RANDOM_GROUP_ID_LENGTH);
    }
}
