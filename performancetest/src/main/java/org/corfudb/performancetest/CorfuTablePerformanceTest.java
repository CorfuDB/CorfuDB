package org.corfudb.performancetest;

import com.codahale.metrics.MetricRegistry;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created by Lin Dong on 10/18/19.
 */

@Slf4j
public class CorfuTablePerformanceTest extends PerformanceTest{
    private final long seed;
    private final int randomBoundary;
    private final long time;
    public static final int MILLI_TO_SECOND = 1000;
    private final int keyNum;
    private final int valueSize;
    private final Random random;
    private final KeyValueManager keyValueManager;

    public CorfuTablePerformanceTest() {
        seed = Long.parseLong(PROPERTIES.getProperty("tableSeed", "1024"));
        randomBoundary = Integer.parseInt(PROPERTIES.getProperty("tableRandomBoundary","10"));
        time = Long.parseLong(PROPERTIES.getProperty("tableTime", "100"));
        keyNum = Integer.parseInt(PROPERTIES.getProperty("keyNum", "10"));
        valueSize = Integer.parseInt(PROPERTIES.getProperty("valueSize", "1024"));
        random = new Random(seed);
        keyValueManager = new org.corfudb.performancetest.KeyValueManager(keyNum, valueSize);
    }

    private CorfuTable<String, String> buildTable(String name) {
        return runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName(name)
                .open();
    }

    private void putTable(CorfuTable corfuTable, int numRequests) {
        for (int i = 0; i < numRequests; i++) {
            String key = keyValueManager.generateKey();
            String value = keyValueManager.generateValue();
            corfuTable.put(key, value);
        }
    }

    private void getTable(CorfuTable corfuTable, int numRequests) {
        for (int i = 0; i < numRequests; i++) {
            String key = keyValueManager.getKey();
            corfuTable.get(key);
        }
    }

    private Thread createThread(CorfuTable table, long start) {
        return new Thread(() -> {
            long duration = 0;
            while ( duration <=  time) {
                duration = (System.currentTimeMillis() - start) / MILLI_TO_SECOND;
                try {
                    putTable(table, random.nextInt(randomBoundary));
                    getTable(table, random.nextInt(randomBoundary));
                } catch (Exception e) {
                    log.error("Operation failed with", e);
                }
            }
        });
    }

    @Test
    public void CorfuTable1Thread1TableTest () throws IOException, InterruptedException {
        setMetricsReportFlags("corfutable-1-1");
        Process server = runServer();
        runtime = initRuntime();
        CorfuTable<String, String>
                corfuTable = buildTable("table1");
        long start = System.currentTimeMillis();
        while (true) {
            if ((System.currentTimeMillis() - start) / MILLI_TO_SECOND > time) {
                break;
            }
            putTable(corfuTable, random.nextInt(randomBoundary));
            getTable(corfuTable, random.nextInt(randomBoundary));
        }
        killServer(server);
    }

    private void runTableOps(int numThreads, int numTables) throws InterruptedException {
        CorfuTable<String, String>[] corfuTables = new CorfuTable[numTables];
        for (int i = 0; i < numTables; i++) {
            corfuTables[i] = buildTable("table" + i);
        }
        Thread[] threads = new Thread[numThreads];
        long start = System.currentTimeMillis();
        for (int i = 0; i < numThreads; i++) {
            threads[i] = createThread(corfuTables[i], start);
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
    }

    @Test
    public void CorfuTable10Thread1TableTest () throws IOException, InterruptedException {
        setMetricsReportFlags("corfutable-10-1");
        Process server = runServer();
        runtime = initRuntime();
        runTableOps(1, 10);
        killServer(server);
    }

    @Test
    public void CorfuTable10Thread10Table() throws IOException, InterruptedException {
        setMetricsReportFlags("corfutable-10-10");
        Process server = runServer();
        runtime = initRuntime();
        runTableOps(10, 10);
        killServer(server);
    }
}
