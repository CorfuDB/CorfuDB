package org.corfudb.runtime.collections;

import com.google.gson.JsonObject;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Minimalistic stats to capture time taken.
 * <p>
 * created by hisundar 2020-09-20
 */
public class TimingHistogram {
    private final String tableName;
    private final String metricsName;
    public TimingHistogram(String metricName, String tableName) {
        this.metricsName = metricName;
        this.tableName = tableName;
    }

    /**
     * Record the time elapsed. Recompute stats.
     *
     * @param sample - A measurement sample to time.
     */
    public void update(Optional<Timer.Sample> sample) {
        MicroMeterUtils.time(sample, this.metricsName, "table.name", this.tableName);
    }

    /**
     * Convert elapsed time in nanoseconds into string with human readable suffix.
     *
     * @param nanos - avg, max or min to suffix
     * @return the stat param with the coarsest time suffix
     */
    public static String coarsestGranularString(double nanos) {
        long secs = TimeUnit.NANOSECONDS.toSeconds((long) nanos);
        if (secs > 0.0) {
            return secs + "s";
        }
        long millis = TimeUnit.NANOSECONDS.toMillis((long) nanos);
        if (millis > 0.0) {
            return millis + "ms";
        }
        long micros = TimeUnit.NANOSECONDS.toMicros((long) nanos);
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

        Optional<Timer> timer =
                MicroMeterUtils.createOrGetTimer(this.metricsName, "table.name", this.tableName);

        if (!timer.isPresent()) {
            return jsonObject;
        }
        HistogramSnapshot snapshot = timer.get().takeSnapshot();
        if (snapshot.total() > 0) {
            jsonObject.addProperty("num", snapshot.total());
            jsonObject.addProperty("avg", coarsestGranularString(snapshot.mean()));
            jsonObject.addProperty("max", coarsestGranularString(snapshot.max()));
            jsonObject.addProperty("p99", coarsestGranularString(snapshot.percentileValues()[2].value()));
        }
        return jsonObject;
    }

    /**
     * @return - return a Json string representation of the histogram with readable values.
     */
    @Override
    public String toString() {
        return this.asJsonObject().toString();
    }
}
