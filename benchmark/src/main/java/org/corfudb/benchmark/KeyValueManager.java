package org.corfudb.benchmark;

import java.util.*;

public class KeyValueManager {
    //long size; // enum
    int capacity;
    List<String> keySet;
    Random random;

    KeyValueManager(int capacity) {
        this.capacity = capacity;
        keySet = new ArrayList<>();
        random = new Random();
    }

    String generateKey() {
        int index = random.nextInt(this.capacity);
        return "key_" + index;
    }

    void addKey(String key) {
        keySet.add(key);
    }

    String getKey() {
        return keySet.get(random.nextInt(keySet.size()));
    }


    String generateValue() {
        // 112B
        return UUID.randomUUID().toString();
    }

}
