package org.corfudb.performancetest;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by Lin Dong on 10/18/19.
 */

@Slf4j
public class CorfuTablePerformanceTest {
    private String endPoint = "localhost:9000";
    private int metricsPort = 1000;
    private long seed = 1024;
    private int randomBoundary = 100;
    private long time = 1000;
    private int milliToSecond = 1000;
    private int keyNum = 10;
    private int valueSize = 1024;
    private CorfuRuntime runtime;
    private Random random;
    private static final Properties PROPERTIES = new Properties();
    private KeyValueManager keyValueManager;

    public CorfuTablePerformanceTest() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("PerformanceTest.properties");
        try {
            PROPERTIES.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        loadProperties();
        random = new Random(seed);
        keyValueManager = new org.corfudb.performancetest.KeyValueManager(keyNum, valueSize);
    }

    private void loadProperties()  {
        metricsPort = Integer.parseInt(PROPERTIES.getProperty("tableMetricsPort", "1000"));
        endPoint = PROPERTIES.getProperty("endPoint", "localhost:9000");
        seed = Long.parseLong(PROPERTIES.getProperty("tableSeed", "1024"));
        randomBoundary = Integer.parseInt(PROPERTIES.getProperty("tableRandomBoundary","10"));
        time = Long.parseLong(PROPERTIES.getProperty("tableTime", "100"));
        milliToSecond = Integer.parseInt(PROPERTIES.getProperty("milliToSecond", "1000"));
        keyNum = Integer.parseInt(PROPERTIES.getProperty("keyNum", "10"));
        valueSize = Integer.parseInt(PROPERTIES.getProperty("valueSize", "1024"));
    }

    private CorfuRuntime initRuntime() {
        CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
        parameters.setPrometheusMetricsPort(metricsPort);
        CorfuRuntime corfuRuntime = CorfuRuntime.fromParameters(parameters);
        corfuRuntime.addLayoutServer(endPoint);
        corfuRuntime.connect();
        return corfuRuntime;
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
    public void CorfuTable1Thread1Table() {
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
    }

    @Test
    public void CorfuTable10Thread1Table() throws InterruptedException {
        runtime = initRuntime();
        CorfuTable<String, String>
                corfuTable = buildTable("table1");
        Thread[] threads = new Thread[10];
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            threads[i] = createThread(corfuTable, start);
            threads[i].start();
        }
        for (int i = 0; i < 10; i++) {
            threads[i].join();
        }
    }

    @Test
    public void CorfuTable10Thread10Table() throws InterruptedException {
        runtime = initRuntime();
        CorfuTable<String, String>[] corfuTables = new CorfuTable[10];
        for (int i = 0; i < 10; i++) {
            corfuTables[i] = buildTable("table" + i);
        }
        Thread[] threads = new Thread[10];
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            threads[i] = createThread(corfuTables[i], start);
            threads[i].start();
        }
        for (int i = 0; i < 10; i++) {
            threads[i].join();
        }
    }
}
