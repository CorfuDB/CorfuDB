package org.corfudb.performancetest;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.*;

/**
 * Created by Lin Dong on 10/18/19.
 */

public class KeyValueManager {//1 char = 2B
    private final int keyNum;
    private final List<String> keySet;
    private final Random random;
    private final int valueSize;
    private int index;

    KeyValueManager(int keyNum, int valueSize) {
        this.keyNum = keyNum;
        this.valueSize = valueSize;
        keySet = new ArrayList<>();
        random = new Random();
    }

    String generateKey() {
        if (index < keyNum) {
            String key = "key_" + index;
            index++;
            keySet.add(key);
            return key;
        } else {
            return getKey();
        }
    }

    String getKey() {
        return keySet.get(random.nextInt(keySet.size()));
    }

    String generateValue() {
        return RandomStringUtils.random(valueSize, true, true);
    }
}