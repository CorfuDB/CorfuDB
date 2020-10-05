package org.corfudb.runtime.collections;

import com.google.gson.JsonObject;

import java.util.concurrent.atomic.AtomicLong;

/**
 * List of metrics captured at a CorfuStore Table level
 *
 * Created by hisundar on 2020-09-22
 *
 */
public class TableMetrics {
    public final SimpleTimingStat writeOnlyTxnTimes = new SimpleTimingStat();
    public final SimpleTimingStat readOnlyTxnTimes = new SimpleTimingStat();
    public final SimpleTimingStat readWriteTxnTimes = new SimpleTimingStat();
    public final AtomicLong numPuts = new AtomicLong(0L);
    public final AtomicLong numMerges = new AtomicLong(0L);
    public final AtomicLong numTouches = new AtomicLong(0L);
    public final AtomicLong numDeletes = new AtomicLong(0L);
    public final AtomicLong numClears = new AtomicLong(0L);
    public final AtomicLong numGets = new AtomicLong(0L);
    public final AtomicLong numCounts = new AtomicLong(0L);
    public final AtomicLong numKeySets = new AtomicLong(0L);
    public final AtomicLong numScans = new AtomicLong(0L);
    public final AtomicLong numGetByIndex = new AtomicLong(0L);
    public final AtomicLong numTxnAborts = new AtomicLong(0L);
    private final String tableName;

    /**
     * Constructor to get the table name.
     */
    TableMetrics(String fullTableName) {
        this.tableName = fullTableName;
    }

    /**
     * Method to pretty print the metrics into a JSON object.
     * @return metrics as jsonObject
     */
    public JsonObject toJsonObject() {
        JsonObject json = new JsonObject();
        json.addProperty("tableName", tableName);
        json.addProperty("numPuts", numPuts.get());
        json.addProperty("numMerges", numMerges.get());
        json.addProperty("numTouches", numTouches.get());
        json.addProperty("numGets", numGets.get());
        json.addProperty("numDeletes", numDeletes.get());
        json.addProperty("numClears", numClears.get());
        json.addProperty("numCounts", numCounts.get());
        json.addProperty("numKeySets", numKeySets.get());
        json.addProperty("numGetByIndex", numGetByIndex.get());
        json.addProperty("numScans", numScans.get());
        json.addProperty("numTxnAborts", numTxnAborts.get());
        json.add("writeOnlyTxTimes", writeOnlyTxnTimes.asJsonObject());
        json.add("readOnlyTxTimes", readOnlyTxnTimes.asJsonObject());
        json.add("readWriteTxTimes", readWriteTxnTimes.asJsonObject());
        return json;
    }

    /**
     * Metrics as a simple string
     * @return Json string representation of the metrics.
     */
    public String toString() {
        return this.toJsonObject().toString();
    }
}
