package com.hk.kafkastreams.showcase.client;

import com.hk.kafkastreams.showcase.StreamsExpProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@RequiredArgsConstructor
@Component
public class PrimitiveRecordProducer {

    private final KafkaTemplate<String, Double> kafkaTemplate;
    private final StreamsExpProperties streamsExpProperties;

    public void sendRecord(String key, Double value) {
        log.info("Sending message, key [{}]: value [{}]", key, value);
        kafkaTemplate.send(streamsExpProperties.getPrimitiveRecordSourceTopic(), key, value);
    }

    public void sendRecordWithFuture(String key, Double value) {
        log.info("Sending message, key [{}]: value [{}]", key, value);
        ListenableFuture<SendResult<String, Double>> sendFuture =
                kafkaTemplate.send(streamsExpProperties.getPrimitiveRecordSourceTopic(), key, value);

        sendFuture.addCallback(new ListenableFutureCallback<SendResult<String, Double>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.info("Failed to send value [{}] with key [{}], because [{}]", value, key, ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Double> result) {
                log.info("Sent value [{}] with key [{}] at [{}]", value, key, result.getRecordMetadata().toString());
            }
        });
    }
}
