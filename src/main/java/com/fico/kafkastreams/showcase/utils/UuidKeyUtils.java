package com.fico.kafkastreams.showcase.utils;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.RandomUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RequiredArgsConstructor
public class UuidKeyUtils {

    public static final int DEFAULT_KEYSET_SIZE = 10;

    public List<String> getSingleUuidKey() {
        List<String> uuidKey = new ArrayList<>(1);
        uuidKey.add(UUID.randomUUID().toString());

        return uuidKey;
    }

    public List<String> getMultipleUuidKeys(int count) {
        List<String> uuidKeys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            uuidKeys.add(UUID.randomUUID().toString());
        }

        return uuidKeys;
    }

    public String selectRandomKey(List<String> keys) {
        return keys.get(RandomUtils.nextInt(0, keys.size()));
    }
}
