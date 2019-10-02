package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.util.MetricsUtils;

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

//    public static void main(String[] args) {
//        KeyValueManager keyValueManager = new KeyValueManager(10, 1700);
//        System.out.println(MetricsUtils.sizeOf.deepSizeOf(keyValueManager.generateValue()));
//        System.out.println(MetricsUtils.sizeOf.deepSizeOf(keyValueManager.generateKey()));
//    }
}
