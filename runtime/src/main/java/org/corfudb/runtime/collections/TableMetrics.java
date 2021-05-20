package org.corfudb.runtime.collections;

import com.google.gson.JsonObject;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;

import java.util.Optional;


/**
 * List of metrics captured at a CorfuStore Table level
 * <p>
 * Created by hisundar on 2020-09-22
 */
public class TableMetrics {
    private final String tableName;
    private final Optional<Counter> numPuts;
    private final Optional<Counter> numEnqueues;
    private final Optional<Counter> numMerges;
    private final Optional<Counter> numTouches;
    private final Optional<Counter> numDeletes;
    private final Optional<Counter> numClears;
    private final Optional<Counter> numGets;
    private final Optional<Counter> numCounts;
    private final Optional<Counter> numKeySets;
    private final Optional<Counter> numScans;
    private final Optional<Counter> numEntryLists;
    private final Optional<Counter> numJoins;
    private final Optional<Counter> numGetByIndex;
    private final Optional<Counter> numTxnAborts;

    private final TimingHistogram openTableTimes;
    private final TimingHistogram writeOnlyTxnTimes;
    private final TimingHistogram readOnlyTxnTimes;
    private final TimingHistogram readWriteTxnTimes;

    public void incNumPuts() {
        numPuts.ifPresent(Counter::increment);
    }

    public void incNumEnqueues() {
        numEnqueues.ifPresent(Counter::increment);
    }

    public void incNumMerges() {
        numMerges.ifPresent(Counter::increment);
    }

    public void incNumTouches() {
        numTouches.ifPresent(Counter::increment);
    }

    public void incNumDeletes() {
        numDeletes.ifPresent(Counter::increment);
    }

    public void incNumClears() {
        numClears.ifPresent(Counter::increment);
    }

    public void incNumGets() {
        numGets.ifPresent(Counter::increment);
    }

    public void incNumCounts() {
        numCounts.ifPresent(Counter::increment);
    }

    public void incNumKeySets() {
        numKeySets.ifPresent(Counter::increment);
    }

    public void incNumScans() {
        numScans.ifPresent(Counter::increment);
    }

    public void incNumEntryLists() {
        numEntryLists.ifPresent(Counter::increment);
    }

    public void incNumJoins() {
        numJoins.ifPresent(Counter::increment);
    }

    public void incNumGetByIndexes() {
        numGetByIndex.ifPresent(Counter::increment);
    }

    public void incNumTxnAborts() {
        numTxnAborts.ifPresent(Counter::increment);
    }

    public void recordTableOpenTime(Optional<Timer.Sample> sample) {
        openTableTimes.update(sample);
    }

    public void recordWriteOnlyTxnTime(Optional<Timer.Sample> sample) {
        writeOnlyTxnTimes.update(sample);
    }

    public void recordReadOnlyTxnTime(Optional<Timer.Sample> sample) {
        readOnlyTxnTimes.update(sample);
    }

    public void recordReadWriteTxnTime(Optional<Timer.Sample> sample) {
        readWriteTxnTimes.update(sample);
    }

    /**
     * Constructor to initialize the metric registry.
     */
    TableMetrics(String fullTableName) {
        Optional<MeterRegistry> registryProvider = MeterRegistryProvider.getInstance();
        this.tableName = fullTableName;
        this.numPuts = registryProvider.map(registry ->
                registry.counter("table.numPuts", "tbl", fullTableName));
        this.numEnqueues = registryProvider.map(registry ->
                registry.counter("table.numEnqueues", "tbl", fullTableName));
        this.numMerges = registryProvider.map(registry ->
                registry.counter("table.numMerges", "tbl", fullTableName));
        this.numTouches = registryProvider.map(registry ->
                registry.counter("table.numTouches", "tbl", fullTableName));
        this.numDeletes = registryProvider.map(registry ->
                registry.counter("table.numDeletes", "tbl", fullTableName));
        this.numClears = registryProvider.map(registry ->
                registry.counter("table.numClears", "tbl", fullTableName));
        this.numGets = registryProvider.map(registry ->
                registry.counter("table.numGets", "tbl", fullTableName));
        this.numCounts = registryProvider.map(registry ->
                registry.counter("table.numCounts", "tbl", fullTableName));
        this.numKeySets = registryProvider.map(registry ->
                registry.counter("table.numKeySets", "tbl", fullTableName));
        this.numScans = registryProvider.map(registry ->
                registry.counter("table.numScans", "tbl", fullTableName));
        this.numEntryLists = registryProvider.map(registry ->
                registry.counter("table.numEntryLists", "tbl", fullTableName));
        this.numJoins = registryProvider.map(registry ->
                registry.counter("table.numJoins", "tbl", fullTableName));
        this.numGetByIndex = registryProvider.map(registry ->
                registry.counter("table.numGetByIndex", "tbl", fullTableName));
        this.numTxnAborts = registryProvider.map(registry ->
                registry.counter("table.numTxnAborts", "tbl", fullTableName));

        this.openTableTimes = new TimingHistogram("openTable", fullTableName);
        this.writeOnlyTxnTimes = new TimingHistogram("writeTxn", fullTableName);
        this.readOnlyTxnTimes = new TimingHistogram("readTxn", fullTableName);
        this.readWriteTxnTimes = new TimingHistogram("readWriteTxn", fullTableName);
    }

    /**
     * Method to pretty print the metrics into a JSON object.
     *
     * @return metrics as jsonObject
     */
    public JsonObject toJsonObject() {
        JsonObject json = new JsonObject();
        json.addProperty("tableName", tableName);
        json.addProperty("numPuts", numPuts.map(Counter::count).orElse(0.0));
        json.addProperty("numEnqueues", numEnqueues.map(Counter::count).orElse(0.0));
        json.addProperty("numMerges", numMerges.map(Counter::count).orElse(0.0));
        json.addProperty("numTouches", numTouches.map(Counter::count).orElse(0.0));
        json.addProperty("numGets", numGets.map(Counter::count).orElse(0.0));
        json.addProperty("numDeletes", numDeletes.map(Counter::count).orElse(0.0));
        json.addProperty("numClears", numClears.map(Counter::count).orElse(0.0));
        json.addProperty("numCounts", numCounts.map(Counter::count).orElse(0.0));
        json.addProperty("numKeySets", numKeySets.map(Counter::count).orElse(0.0));
        json.addProperty("numGetByIndex", numGetByIndex.map(Counter::count).orElse(0.0));
        json.addProperty("numScans", numScans.map(Counter::count).orElse(0.0));
        json.addProperty("numEntryLists", numEntryLists.map(Counter::count).orElse(0.0));
        json.addProperty("numJoins", numJoins.map(Counter::count).orElse(0.0));
        json.addProperty("numTxnAborts", numTxnAborts.map(Counter::count).orElse(0.0));
        json.add("openTable", readWriteTxnTimes.asJsonObject());
        json.add("writeOnlyTxn", writeOnlyTxnTimes.asJsonObject());
        json.add("readOnlyTxn", readOnlyTxnTimes.asJsonObject());
        json.add("dirtyReadTxn", readWriteTxnTimes.asJsonObject());
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
