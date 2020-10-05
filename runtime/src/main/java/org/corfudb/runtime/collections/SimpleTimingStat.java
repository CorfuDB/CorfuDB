package org.corfudb.runtime.collections;

import com.google.gson.JsonObject;

import java.util.concurrent.TimeUnit;

/**
 * Minimalistic stats to capture time taken.
 *
 * created by hisundar 2020-09-20
 */
public class SimpleTimingStat {
    private long num = 0L;
    private long avg = 0L;
    private long max = Long.MIN_VALUE;
    private long min = Long.MAX_VALUE;

    /**
     * Record the time elapsed. Recompute stats.
     *
     * @param val - time elapsed in nano seconds
     */
    public synchronized void set(long val) {
        num++;
        if (val > max) {
            max = val;
        }
        if (val < min) {
            min = val;
        }
        avg = ((num-1)*avg + val)/num;
    }

    /**
     * Convert elapsed time in nanoseconds into string with human readable suffix.
     *
     * @param nanos - avg, max or min to suffix
     * @return the stat param with the coarsest time suffix
     */
    public static String coarsestGranularString(long nanos) {
        long secs = TimeUnit.NANOSECONDS.toSeconds(nanos);
        if (secs > 0) {
            return secs + "s";
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        if (millis > 0) {
            return millis + "ms";
        }
        long micros = TimeUnit.NANOSECONDS.toMicros(nanos);
        if (micros > 0) {
            return micros + "us";
        }
        return nanos + "ns";
    }

    /**
     * @return Json representation of this class with the right suffixes for times
     */
    public synchronized JsonObject asJsonObject() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("num", num);
        if (num > 0) {
            jsonObject.addProperty("avg", coarsestGranularString(avg));
            jsonObject.addProperty("max", coarsestGranularString(max));
            jsonObject.addProperty("min", coarsestGranularString(min));
        }
        return  jsonObject;
    }
}
