package com.fico.kafkastreams.showcase.streams;

import com.fico.kafkastreams.showcase.StreamsExpProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@RestController
@RequestMapping(value = "/streams")
public class StreamsController {

    private final StreamsExpProperties streamsExpProperties;

    private final PrimitiveProcessingService primitiveProcessingService;

    @GetMapping("/expectedTopics")
    public ResponseEntity<Map<String, String>> expectedTopics() {
        Map<String, String> expectedTopics = new HashMap<>();
        expectedTopics.put("primitive record source topic", streamsExpProperties.getPrimitiveRecordSourceTopic());
        expectedTopics.put("primitive record sink topic", streamsExpProperties.getPrimitiveRecordSinkTopic());
        expectedTopics.put("simple record source topic", streamsExpProperties.getSimpleRecordSourceTopic());
        expectedTopics.put("simple record sink topic", streamsExpProperties.getSimpleRecordSinkTopic());
        expectedTopics.put("single partition source topic", streamsExpProperties.getSinglePartitionSourceTopic());
        expectedTopics.put("single partition sink topic", streamsExpProperties.getSinglePartitionSinkTopic());

        return ResponseEntity.ok(expectedTopics);
    }

    @GetMapping("/metricNames")
    public ResponseEntity<List<String>> getMetrics() {
        return ResponseEntity.ok(primitiveProcessingService.getStreamsMetricsNames());
    }
}
