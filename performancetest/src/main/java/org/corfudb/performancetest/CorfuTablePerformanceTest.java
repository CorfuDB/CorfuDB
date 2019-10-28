package org.corfudb.performancetest;

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
    private long seed;
    private int randomBoundary;
    private long time;
    private int milliToSecond;
    private int keyNum;
    private int valueSize;
    private CorfuRuntime runtime;
    private Random random;
    private KeyValueManager keyValueManager;

    public CorfuTablePerformanceTest() {
        loadProperties();
        random = new Random(seed);
        keyValueManager = new org.corfudb.performancetest.KeyValueManager(keyNum, valueSize);
    }

    private void loadProperties()  {
        seed = Long.parseLong(PROPERTIES.getProperty("tableSeed", "1024"));
        randomBoundary = Integer.parseInt(PROPERTIES.getProperty("tableRandomBoundary","10"));
        time = Long.parseLong(PROPERTIES.getProperty("tableTime", "100"));
        milliToSecond = Integer.parseInt(PROPERTIES.getProperty("milliToSecond", "1000"));
        keyNum = Integer.parseInt(PROPERTIES.getProperty("keyNum", "10"));
        valueSize = Integer.parseInt(PROPERTIES.getProperty("valueSize", "1024"));
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
                duration = (System.currentTimeMillis() - start) / milliToSecond;
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
    public void CorfuTable1Thread1Table() throws IOException, InterruptedException {
        setMetricsReportFlags("corfutable-1-1");
        Process server = runServer();
        runtime = initRuntime();
        CorfuTable<String, String>
                corfuTable = buildTable("table1");
        long start = System.currentTimeMillis();
        while (true) {
            if ((System.currentTimeMillis() - start) / milliToSecond > time) {
                break;
            }
            putTable(corfuTable, random.nextInt(randomBoundary));
            getTable(corfuTable, random.nextInt(randomBoundary));
        }
        killServer();
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
    public void CorfuTable10Thread1Table() throws InterruptedException, IOException {
        setMetricsReportFlags("corfutable-10-1");
        Process server = runServer();
        runtime = initRuntime();
        runTableOps(1, 10);
        killServer();
    }

    @Test
    public void CorfuTable10Thread10Table() throws InterruptedException, IOException {
        setMetricsReportFlags("corfutable-10-10");
        Process server = runServer();
        runtime = initRuntime();
        runTableOps(10, 10);
        killServer();
    }
}
