package com.fico.kafkastreams.showcase.client;

import com.fico.kafkastreams.showcase.utils.UuidKeyUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping(value = "/client")
public class ClientController {

    private final UuidKeyUtils uuidKeyUtils;
    private final PrimitiveRecordProducerService primitiveRecordProducerService;

    @GetMapping(value = "/keys")
    public ResponseEntity<List<String>> getUuids(@RequestParam("count") int count) {
        log.info("Received request for {} random UUID keys", count);
        return ResponseEntity.ok(uuidKeyUtils.getMultipleUuidKeys(count));
    }

    @GetMapping(value = "/key")
    public ResponseEntity<List<String>> getUuid() {
        log.info("Received request for 1 random UUID key");
        return ResponseEntity.ok(uuidKeyUtils.getSingleUuidKey());
    }

    @GetMapping("/generateSimpleRecords")
    public ResponseEntity<List<String>> generateRandomRecords(@RequestParam("recordCount") int recordCount,
                                                              @RequestParam("keyCount") int keyCount) {
        log.info("Received request for generating {} random records using a key pool size of {}", recordCount, keyCount);
        List<String> randomKeys = uuidKeyUtils.getMultipleUuidKeys(keyCount);
        primitiveRecordProducerService.generateRecords(recordCount, randomKeys);

        return ResponseEntity.ok(randomKeys);
    }
}
