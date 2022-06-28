package com.fico.kafkastreams.showcase.client;

import com.fico.kafkastreams.showcase.StreamsExpProperties;
import com.fico.kafkastreams.showcase.utils.UuidKeyUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;

@RequiredArgsConstructor
@Slf4j
@Service
public class PrimitiveRecordProducerService {

    private final StreamsExpProperties streamsExpProperties;
    private final PrimitiveRecordProducer primitiveRecordProducer;

    private final UuidKeyUtils uuidKeyUtils;

    @Async
    public void generateRecords(int recordCount, int keyCount) {
        List<String> keys = uuidKeyUtils.getMultipleUuidKeys(keyCount);

        String currentKey;
        double currentValue;
        log.info("generating {} key-value pairs using key randomly selected from a pool of {} and a random double value", recordCount, keyCount);
        for (int i = 0; i < recordCount; i++) {
            currentKey = uuidKeyUtils.selectRandomKey(keys);
            currentValue = RandomUtils.nextDouble(0, 1000);
            log.info("sending key:value {}:{} to kafka topic", currentKey, currentValue);
        }
    }

    @Async
    public void generateRecords(int recordCount) {
        generateRecords(recordCount, 1);
    }

    @Async
    public void generateRecords(int recordCount, List<String> keys) {
        String currentKey;
        double currentValue;
        log.info("generating {} key-value pairs using key randomly selected from a pool of {} and a random double value", recordCount, keys.size());
        for (int i = 0; i < recordCount; i++) {
            currentKey = uuidKeyUtils.selectRandomKey(keys);
            currentValue = RandomUtils.nextDouble(0, 1000);
            log.info("sending key:value {}:{} to kafka topic", currentKey, currentValue);
        }
    }
}
