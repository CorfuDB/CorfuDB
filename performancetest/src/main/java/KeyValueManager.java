package org.corfudb.performancetest;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.*;

public class KeyValueManager {//1 char = 2B
    int keyNum;
    List<String> keySet;
    Random random;
    int valueSize;
    int index;
    private final static int SEEDSIZE = 26;

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