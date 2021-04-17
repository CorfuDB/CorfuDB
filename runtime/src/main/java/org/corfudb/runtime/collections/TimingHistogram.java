package org.corfudb.runtime.collections;

import com.google.gson.JsonObject;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;

import java.util.concurrent.TimeUnit;
import java.util.Optional;

/**
 * Minimalistic stats to capture time taken.
 *
 * created by hisundar 2020-09-20
 */
public class TimingHistogram {
    private final Optional<DistributionSummary> timer;

    public TimingHistogram(String metricName, String tag) {
        this.timer = MeterRegistryProvider.getInstance().map(registry ->
                DistributionSummary
                        .builder(metricName)
                        .tag("time", tag)
                        .publishPercentiles(0.50, 0.95, 0.99)
                        .publishPercentileHistogram()
                        .baseUnit("s")
                        .register(registry));
    }

    /**
     * Record the time elapsed. Recompute stats.
     *
     * @param val - time elapsed in nano seconds
     */
    public void update(long val) {
        if (!timer.isPresent()) {
            return;
        }
        timer.get().record(val);
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
        if (!timer.isPresent()) {
            return jsonObject;
        }
        HistogramSnapshot snapshot = timer.get().takeSnapshot();
        if (snapshot.total() > 0) {
            jsonObject.addProperty("num", snapshot.total());
            jsonObject.addProperty("avg", coarsestGranularString(snapshot.mean()));
            jsonObject.addProperty("max", coarsestGranularString(snapshot.max()));
            jsonObject.addProperty("p95", coarsestGranularString(snapshot.percentileValues()[1].value()));
            jsonObject.addProperty("p99", coarsestGranularString(snapshot.percentileValues()[2].value()));
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
