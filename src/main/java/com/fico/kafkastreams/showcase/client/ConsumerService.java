package com.fico.kafkastreams.showcase.client;

import com.fico.kafkastreams.showcase.utils.ClientConfigurationUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class ConsumerService {

    public void makeConsumer() {
        final Properties consumerProperties = ClientConfigurationUtils.consumerProperties();
        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {

        }
    }

}
