package org.corfudb.runtime.collections;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;


/**
 * List of metrics captured at a CorfuStore Table level
 *
 * Created by hisundar on 2020-09-22
 *
 */
public class TableMetrics {
    private final String tableName;
    private final Counter numPuts;
    private final Counter numEnqueues;
    private final Counter numMerges;
    private final Counter numTouches;
    private final Counter numDeletes;
    private final Counter numClears;
    private final Counter numGets;
    private final Counter numCounts;
    private final Counter numKeySets;
    private final Counter numScans;
    private final Counter numJoins;
    private final Counter numGetByIndex;
    private final Counter numTxnAborts;

    private final TimingHistogram writeOnlyTxnTimes;
    private final TimingHistogram readOnlyTxnTimes;
    private final TimingHistogram readWriteTxnTimes;

    public void incNumPuts() {
        numPuts.inc();
    }

    public void incNumEnqueues() {
        numEnqueues.inc();
    }

    public void incNumMerges() {
        numMerges.inc();
    }

    public void incNumTouches() {
        numTouches.inc();
    }

    public void incNumDeletes() {
        numDeletes.inc();
    }

    public void incNumClears() {
        numClears.inc();
    }

    public void incNumGets() {
        numGets.inc();
    }

    public void incNumCounts() {
        numCounts.inc();
    }

    public void incNumKeySets() {
        numKeySets.inc();
    }

    public void incNumScans() {
        numScans.inc();
    }

    public void incNumJoins() {
        numJoins.inc();
    }

    public void incNumGetByIndexes() {
        numGetByIndex.inc();
    }

    public void incNumTxnAborts() {
        numTxnAborts.inc();
    }

    public void setWriteOnlyTxnTimes(long elapsedTime) {
        writeOnlyTxnTimes.update(elapsedTime);
    }

    public void setReadOnlyTxnTimes(long elapsedTime) {
        readOnlyTxnTimes.update(elapsedTime);
    }

    public void setReadWriteTxnTimes(long elapsedTime) {
        readWriteTxnTimes.update(elapsedTime);
    }

    /**
     * Constructor to initialize the metric registry.
     */
    TableMetrics(String fullTableName, MetricRegistry registry) {
        this.tableName = fullTableName;
        this.numPuts = registry.counter(fullTableName+"_numPuts");
        this.numEnqueues = registry.counter(fullTableName+"_numEnqueues");
        this.numMerges = registry.counter(fullTableName+"_numMerges");
        this.numTouches = registry.counter(fullTableName+"_numTouches");
        this.numDeletes = registry.counter(fullTableName+"_numDeletes");
        this.numClears = registry.counter(fullTableName+"_numClears");
        this.numGets = registry.counter(fullTableName+"_numGets");
        this.numCounts = registry.counter(fullTableName+"_numCounts");
        this.numKeySets = registry.counter(fullTableName+"_numKeySets");
        this.numScans = registry.counter(fullTableName+"_numScans");
        this.numJoins = registry.counter(fullTableName+"_numJoins");
        this.numGetByIndex = registry.counter(fullTableName+"_numGetByIndex");
        this.numTxnAborts = registry.counter(fullTableName+"_numTxnAborts");

        this.writeOnlyTxnTimes = new TimingHistogram(registry.histogram(fullTableName+"_writeOnlyTxnTimes"));
        this.readOnlyTxnTimes = new TimingHistogram(registry.histogram(fullTableName+"_readOnlyTxnTimes"));
        this.readWriteTxnTimes = new TimingHistogram(registry.histogram(fullTableName+"_readWriteTxnTimes"));
    }

    /**
     * Method to pretty print the metrics into a JSON object.
     * @return metrics as jsonObject
     */
    public JsonObject toJsonObject() {
        JsonObject json = new JsonObject();
        json.addProperty("tableName", tableName);
        json.addProperty("numPuts", numPuts.getCount());
        json.addProperty("numEnqueues", numEnqueues.getCount());
        json.addProperty("numMerges", numMerges.getCount());
        json.addProperty("numTouches", numTouches.getCount());
        json.addProperty("numGets", numGets.getCount());
        json.addProperty("numDeletes", numDeletes.getCount());
        json.addProperty("numClears", numClears.getCount());
        json.addProperty("numCounts", numCounts.getCount());
        json.addProperty("numKeySets", numKeySets.getCount());
        json.addProperty("numGetByIndex", numGetByIndex.getCount());
        json.addProperty("numScans", numScans.getCount());
        json.addProperty("numJoins", numJoins.getCount());
        json.addProperty("numTxnAborts", numTxnAborts.getCount());
        json.add("writeOnlyTxTimes", writeOnlyTxnTimes.asJsonObject());
        json.add("readOnlyTxTimes", readOnlyTxnTimes.asJsonObject());
        json.add("dirtyReadTxTimes", readWriteTxnTimes.asJsonObject());
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
