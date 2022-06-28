package com.fico.kafkastreams.showcase.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@EqualsAndHashCode
@ToString
@NoArgsConstructor
public class SimpleRecord implements Serializable {
    private String timestamp;
    private String recordId;
    private String batchId;
    private String processingId;
    private String keyA;
    private String keyB;
    private String keyC;
    private double floatingPointValue;
    private double collectedFloatingPointValue;
    private long integerValue;
    private long collectedIntegerValue;
}
