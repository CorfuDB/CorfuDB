package org.corfudb.benchmark;

import java.util.*;

public class KeyValueManager {//1 char = 2B
    int keyNum;
    List<String> keySet;
    Random random;
    int valueSize;
    private final static int SEEDSIZE = 26;

    KeyValueManager(int keyNum, int valueSize) {
        this.keyNum = keyNum;
        this.valueSize = valueSize;
        keySet = new ArrayList<>();
        random = new Random();
    }

    String generateKey() {
        int index = random.nextInt(this.keyNum);
        String key = "key_" + index;
        keySet.add(key);
        return key;
    }

    String getKey() {
        return keySet.get(random.nextInt(keySet.size()));
    }

    String generateValue() {
        StringBuilder stringBuilder = new StringBuilder(valueSize);
        for (int i = 0; i < valueSize / 2; i++) {
            int seed = random.nextInt(SEEDSIZE);
            char ch = (char) ('a' + seed);
            stringBuilder.append(ch);
        }
        return stringBuilder.toString();
    }
//    public static void main(String[] args) {
//        KeyValueManager keyValueManager = new KeyValueManager(10);
//        System.out.println(MetricsUtils.sizeOf.deepSizeOf(keyValueManager.generateValue()));
//    }
}
