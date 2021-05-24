package org.corfudb.runtime.collections;

import com.google.gson.JsonObject;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;

import java.util.Optional;

public class CorfuStoreMetrics {
    // Keep track of the number of read entries to compute a stream's highestSequenceNumber
    private final Optional<DistributionSummary> numberBatchReads;
    private final Optional<DistributionSummary> numberReads;
    private final TimingHistogram highestSequenceNumberDuration;

    private final Optional<Counter> openTableCounter;

    public void recordBatchReads(int numBatches) {
        numberBatchReads.ifPresent(counter -> counter.record(numBatches));
    }

    public void recordNumberReads(int reads) {
        numberReads.ifPresent(counter -> counter.record(reads));
    }

    public void recordTableCount() {
        openTableCounter.ifPresent(Counter::increment);
    }

    public void recordHighestSequenceNumberDuration(Optional<Timer.Sample> startTime) {
        highestSequenceNumberDuration.update(startTime);
    }

    /**
     * Constructor to initialize the metric registry.
     */
    CorfuStoreMetrics() {
        Optional<MeterRegistry> registryProvider = MeterRegistryProvider.getInstance();
        this.numberBatchReads = registryProvider.map(registry -> DistributionSummary
                .builder("highestSeqNum.numberBatchReads")
                .publishPercentiles(0.5, 0.99)
                .description("Number of batches read before finding highest DATA sequence number")
                .register(registry));
        this.numberReads = registryProvider.map(registry ->
                DistributionSummary
                        .builder("highestSeqNum.numberReads")
                        .publishPercentiles(0.5, 0.99)
                        .description("Number of addresses read in batches before finding highest DATA sequence number")
                        .register(registry));
        this.openTableCounter = registryProvider.map(registry ->
                registry.counter("open_tables.count"));

        this.highestSequenceNumberDuration = new TimingHistogram("highestSequenceNumberDuration", "CorfuStore");
    }

    /**
     * Method to pretty print the metrics into a JSON object.
     *
     * @return metrics as jsonObject
     */
    public JsonObject toJsonObject() {
        JsonObject json = new JsonObject();
        json.addProperty("numberBatchReads_count", numberBatchReads.map(DistributionSummary::count).orElse(0L));
        json.addProperty("numberBatchReads_max", numberBatchReads.map(DistributionSummary::max).orElse(0.0));
        json.addProperty("numberBatchReads_mean", numberBatchReads.map(DistributionSummary::mean).orElse(0.0));
        json.addProperty("numberReads_count", numberReads.map(DistributionSummary::count).orElse(0L));
        json.addProperty("numberReads_max", numberReads.map(DistributionSummary::max).orElse(0.0));
        json.addProperty("numberReads_mean", numberReads.map(DistributionSummary::mean).orElse(0.0));
        json.addProperty("openTableCounter", openTableCounter.map(Counter::count).orElse(0.0));
        json.add("highestSequenceNumberDuration", highestSequenceNumberDuration.asJsonObject());
        return json;
    }

    /**
     * Metrics as a simple string
     *
     * @return Json string representation of the metrics.
     */
    @Override
    public String toString() {
        return this.toJsonObject().toString();
    }
}
