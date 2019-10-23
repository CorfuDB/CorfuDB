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
    public String endPoint = "localhost:9000";
    public int metricsPort = 1000;
    public long seed = 1024;
    public int randomBoundary = 100;
    public long time = 1000;
    public int milliToSecond = 1000;
    private int keyNum = 10;
    private int valueSize = 1024;
    private CorfuRuntime runtime;
    private Random random;
    public static final Properties PROPERTIES = new Properties();
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

    private String getProperty(String key) {
        if (PROPERTIES.containsKey(key)) {
            return (String) PROPERTIES.get(key);
        }
        System.out.println("print null");
        return null;
    }
    private void loadProperties()  {
        metricsPort = Integer.parseInt(getProperty("tableMetricsPort"));
        endPoint = getProperty("endPoint");
        seed = Long.parseLong(getProperty("tableSeed"));
        randomBoundary = Integer.parseInt(getProperty("tableRandomBoundary"));
        time = Long.parseLong(getProperty("tableTime"));
        milliToSecond = Integer.parseInt(getProperty("milliToSecond"));
        keyNum = Integer.parseInt(getProperty("keyNum"));
        valueSize = Integer.parseInt(getProperty("valueSize"));
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
        System.out.println("=========put operation =========="+numRequests);
        for (int i = 0; i < numRequests; i++) {
            String key = keyValueManager.generateKey();
            String value = keyValueManager.generateValue();
            //try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTablePutTimer)) {
            corfuTable.put(key, value);
            //}
        }
    }

    private void getTable(CorfuTable corfuTable, int numRequests) {
        System.out.println("=========get operation =========="+numRequests);
        for (int i = 0; i < numRequests; i++) {
            String key = keyValueManager.getKey();
            System.out.println("get value for key: " + key);
            //try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTableGetTimer)) {
            Object value = corfuTable.get(key);
            //System.out.println(value);
            if (value == null) {
                System.out.println("no such key");
            } else {
                System.out.println("get value: "+value);
            }
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
            System.out.println("finish test");
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
                System.out.println("finish test");
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
            int id = i;
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
            int id = i;
            threads[i] = createThread(corfuTables[i], start);
            threads[i].start();
        }
        for (int i = 0; i < 10; i++) {
            threads[i].join();
        }
    }
}
