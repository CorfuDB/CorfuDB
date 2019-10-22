package org.corfudb.performancetest;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Lin Dong on 10/18/19.
 */

@Slf4j
public class CorfuTablePerformanceTest {
    public final static String DEFAULT_ENDPOINT = "localhost:9000";
    public final static int METRICS_PORT = 1000;
    public final static long SEED = 1024;
    public final static int RANDOM_BOUNDARY = 100;
    public final static long TIME = 1000;
    public final static int MILLI_TO_SECOND = 1000;
    private int numRuntimes;
    private int numThreads;
    private int numTables;
    private int keyNum;
    private int valueSize;
    private CorfuRuntime runtime;
    private Random random;
    public static final Properties PROPERTIES = new Properties();
    private org.corfudb.performancetest.KeyValueManager keyValueManager;

    public CorfuTablePerformanceTest() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("PerformanceTest.properties");

        try {
            PROPERTIES.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        loadProperties();
        random = new Random(SEED);
        keyValueManager = new org.corfudb.performancetest.KeyValueManager(keyNum, valueSize);
    }

    private void loadProperties()  {

    }

    private CorfuRuntime initRuntime() {
        CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
        parameters.setPrometheusMetricsPort(METRICS_PORT);
        CorfuRuntime corfuRuntime = CorfuRuntime.fromParameters(parameters);
        corfuRuntime.addLayoutServer(DEFAULT_ENDPOINT);
        corfuRuntime.connect();
        return corfuRuntime;
    }

    private void putTable(CorfuTable corfuTable, int numRequests) {
        for (int i = 0; i < numRequests; i++) {
            String key = keyValueManager.generateKey();
            String value = keyValueManager.generateValue();
            //try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTablePutTimer)) {
            corfuTable.put(key, value);
            //}
        }
    }

    private void getTable(CorfuTable corfuTable, int numRequests) {
        for (int i = 0; i < numRequests; i++) {
            String key = keyValueManager.getKey();
            log.info("get value for key: " + key);
            //try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTableGetTimer)) {
            Object value = corfuTable.get(key);
            if (value == null) {
                log.info("not such key");
            } else {
                log.info("get value: "+value);
            }
            //}
        }
    }

    @Test
    public void CorfuTable1Thread1Table() {
        runtime = initRuntime();
        CorfuTable<String, String>
                corfuTable = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("buildtest")
                .open();
        long start = System.currentTimeMillis();
        while (true) {
            if ((System.currentTimeMillis() - start) / MILLI_TO_SECOND >  TIME) {
                System.out.println("finish test");
                break;
            }
            putTable(corfuTable, random.nextInt(RANDOM_BOUNDARY));
            getTable(corfuTable, random.nextInt(RANDOM_BOUNDARY));
        }
    }

    private void runWorkers(ExecutorService workers, CorfuTable corfuTable) {
        for (int i = 0; i < numThreads; i++) {
            workers.execute(() -> {
                try {
                    long start = System.currentTimeMillis();
                    putTable(corfuTable, random.nextInt(RANDOM_BOUNDARY));
                    getTable(corfuTable, random.nextInt(RANDOM_BOUNDARY));
                    long latency = System.currentTimeMillis() - start;
                    log.info("operation latency(milliseconds): " + latency);
                } catch (Exception e) {
                    log.error("Operation failed with", e);
                }
            });
        }
    }

    @Test
    public void CorfuTable10Thread1Table() {
        runtime = initRuntime();
        CorfuTable<String, String>
                corfuTable = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("buildtest")
                .open();
        ExecutorService workers = Executors.newFixedThreadPool(numThreads);
        long start = System.currentTimeMillis();
        while (true) {
            if ((System.currentTimeMillis() - start) / MILLI_TO_SECOND >  TIME) {
                System.out.println("finish test");
                break;
            }
            runWorkers(workers, corfuTable);
        }
    }

    @Test
    public void CorfuTable10Thread10Table() {
        runtime = initRuntime();
        CorfuTable<String, String>[] corfuTables = new CorfuTable[10];
        for (int i = 0; i < 10; i++) {
            corfuTables[i] = runtime.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setStreamName("table"+i)
                    .open();
        }
        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            int id = i;
            threads[i] =  new Thread(() -> {
                try {
                    putTable(corfuTables[id], random.nextInt(RANDOM_BOUNDARY));
                    getTable(corfuTables[id], random.nextInt(RANDOM_BOUNDARY));
                } catch (Exception e) {
                    log.error("Operation failed with", e);
                }
            });
        }
        long start = System.currentTimeMillis();
        int i = 0;
        while (true) {
            if ((System.currentTimeMillis() - start) / MILLI_TO_SECOND >  TIME) {
                System.out.println("finish test");
                break;
            }
            threads[i % 10].start();
            i++;
        }
    }
}
