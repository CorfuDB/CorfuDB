package org.corfudb.runtime.collections;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.google.gson.JsonObject;

import java.util.concurrent.TimeUnit;

/**
 * Minimalistic stats to capture time taken.
 *
 * created by hisundar 2020-09-20
 */
public class TimingHistogram {
    private final Histogram histogram;

    public TimingHistogram(Histogram histogram) {
        this.histogram = histogram;
    }

    /**
     * Record the time elapsed. Recompute stats.
     *
     * @param val - time elapsed in nano seconds
     */
    public void update(long val) {
        histogram.update(val);
    }

    /**
     * Convert elapsed time in nanoseconds into string with human readable suffix.
     *
     * @param nanos - avg, max or min to suffix
     * @return the stat param with the coarsest time suffix
     */
    public static String coarsestGranularString(double nanos) {
        long secs = TimeUnit.NANOSECONDS.toSeconds((long)nanos);
        if (secs > 0.0) {
            return secs + "s";
        }
        long millis = TimeUnit.NANOSECONDS.toMillis((long)nanos);
        if (millis > 0.0) {
            return millis + "ms";
        }
        long micros = TimeUnit.NANOSECONDS.toMicros((long)nanos);
        if (micros > 0.0) {
            return micros + "us";
        }
        return nanos + "ns";
    }

    /**
     * @return Json representation of this class with the right suffixes for times
     */
    public JsonObject asJsonObject() {
        JsonObject jsonObject = new JsonObject();
        final Snapshot snapshot = histogram.getSnapshot();
        jsonObject.addProperty("num", histogram.getCount());
        if (histogram.getCount() > 0) {
            jsonObject.addProperty("avg", coarsestGranularString(snapshot.getMean()));
            jsonObject.addProperty("max", coarsestGranularString(snapshot.getMax()));
            jsonObject.addProperty("min", coarsestGranularString(snapshot.getMin()));
            jsonObject.addProperty("p95", coarsestGranularString(snapshot.get95thPercentile()));
        }
        return  jsonObject;
    }

    /**
     * @return - return a Json string representation of the histogram with readable values.
     */
    @Override
    public String toString() {
        return this.asJsonObject().toString();
    }
}
