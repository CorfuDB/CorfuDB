package org.corfudb.benchmarks.util;

import java.util.Random;

/**
 * Fast data generator, can be used to achieve high throughput for data-intensive operations.
 */
public class DataGenerator {

    private static final char[] CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".toCharArray();
    private static final Random rnd = new Random();

    private static final int PRE_GENERATED_DATASET_SIZE = 16 * 1024 * 1024;
    private static final char[] PRE_GENERATED = generate();

    private DataGenerator() {
        //prevent creating instances
    }

    /**
     * Fast string generator.
     *
     * @param size generated string size
     * @return pseudo random string
     */
    public static String generateDataString(int size) {
        int length = PRE_GENERATED.length;
        int offset = rnd.nextInt(length - size - 1);
        return new String(PRE_GENERATED, offset, size);
    }

    private static char[] generate() {
        char[] result = new char[PRE_GENERATED_DATASET_SIZE];
        for (int i = 0; i < PRE_GENERATED_DATASET_SIZE; i++) {
            result[i] = CHARS[rnd.nextInt(CHARS.length)];
        }

        return result;
    }
}
