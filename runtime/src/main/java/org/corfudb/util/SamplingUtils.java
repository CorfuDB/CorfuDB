package org.corfudb.util;

import static com.carrotsearch.sizeof.RamUsageEstimator.sizeOf;

import java.util.Arrays;

/**
 * Select a random subset k, from a stream that has size n,
 * but n is unknown (i.e. online sampling).
 *
 * Created by maithem on 8/7/17.
 */
public class SamplingUtils {

    private int k;
    private long n;
    private long[] sample;
    private int x;

    public SamplingUtils(int k, long p) {
        this.k = k;
        this.n = 0;
        this.x = 0;
        this.sample = new long[k];
    }

    private void addToSample(Object object) {
        n++;
        long leftLimit = 0L;
        long rightLimit = n;
        long randomLong = leftLimit + (long) (Math.random() * (rightLimit - leftLimit));
        if (x < k) {

            sample[x] = sizeOf(object);
            x++;
        } else if (randomLong < k) {
            sample[(int) randomLong] = sizeOf(object);
        }
    }

    private long getMedian() {
        Arrays.sort(sample);
        return sample[sample.length / 2];
    }

    private long getMean() {
        long sum = 0;
        for (int i = 0; i < sample.length; i++) {
            sum += sample[i];
        }
        return sum/sample.length;
    }
}
