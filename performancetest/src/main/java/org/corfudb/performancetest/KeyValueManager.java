package org.corfudb.performancetest;

import org.apache.commons.lang3.RandomStringUtils;
import java.util.*;

/**
 * KeyValueManager is used for generating and
 * storing key and value strings.
 * @author Lin Dong
 */
public class KeyValueManager {
    private final int keyNum;
    private final List<String> keySet;
    private final Random random;
    private final int valueSize;
    private int index;

    /**
     * Constructor.
     * @param keyNum
     * @param valueSize
     */
    KeyValueManager(int keyNum, int valueSize) {
        this.keyNum = keyNum;
        this.valueSize = valueSize;
        keySet = new ArrayList<>();
        random = new Random();
    }

    /**
     * Generate a key string if the length of keySet doesn't exceed keyNum.
     * @return key string
     */
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

    /**
     * Randomly return a key string from keySet.
     * @return key string
     */
    String getKey() {
        return keySet.get(random.nextInt(keySet.size()));
    }

    /**
     * Randomly generate a value string.
     * @return value string
     */
    String generateValue() {
        return RandomStringUtils.random(valueSize, true, true);
    }
}
