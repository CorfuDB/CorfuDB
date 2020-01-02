package org.corfudb.performancetest;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;
import java.io.IOException;
import java.util.*;

/**
 * CorfuTablePerformanceTest is the subclass of PerformanceTest.
 * This class is used for CorfuTable performance test, mainly for put and get operations.
 * @author Lin Dong
 */
@Slf4j
public class CorfuTablePerformanceTest extends PerformanceTest{
    /**
     * Boundary of the integer generated randomly.
     */
    private final int randomBoundary;
    /**
     * The time each CorfuTable performance test runs.
     */
    private final long time;
    /**
     * Random Object.
     */
    private final Random random;
    /**
     * KeyValueManager for generating and managing key value pairs.
     */
    private final KeyValueManager keyValueManager;

    /**
     * Constructor. Initiate related variables.
     */
    public CorfuTablePerformanceTest() {
        final long seed = Long.parseLong(PROPERTIES.getProperty("tableSeed", "1024"));
        randomBoundary = Integer.parseInt(PROPERTIES.getProperty("tableRandomBoundary","10"));
        time = Long.parseLong(PROPERTIES.getProperty("tableTime", "100"));
        final int keyNum = Integer.parseInt(PROPERTIES.getProperty("keyNum", "10"));
        final int valueSize = Integer.parseInt(PROPERTIES.getProperty("valueSize", "1024"));
        random = new Random(seed);
        keyValueManager = new KeyValueManager(keyNum, valueSize);
    }

    /**
     * Build a new CorfuTable using the given name.
     * @param name the stream name.
     * @return a new CorfuTable.
     */
    private CorfuTable<String, String> buildTable(String name) {
        return runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName(name)
                .open();
    }

    /**
     * Generated a key value pair and put them in the CorfuTable.
     * @param corfuTable the CorfuTable.
     * @param numRequests how many times the function calls put().
     */
    private void putTable(CorfuTable corfuTable, int numRequests) {
        for (int i = 0; i < numRequests; i++) {
            String key = keyValueManager.generateKey();
            String value = keyValueManager.generateValue();
            corfuTable.put(key, value);
        }
    }

    /**
     * Getting an existing key from KeyValueManager and get the corresponding
     * value from the CorfuTable.
     * @param corfuTable the CorfuTable.
     * @param numRequests how many times the function calls get().
     */
    private void getTable(CorfuTable corfuTable, int numRequests) {
        for (int i = 0; i < numRequests; i++) {
            String key = keyValueManager.getKey();
            corfuTable.get(key);
        }
    }

    /**
     * Generate a new Thread which keeps calling putTable() and getTable() for a period.
     * @param table The CorfuTable.
     * @param start start time of a performance test.
     * @return new Thread.
     */
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

    /**
     * Performance test for using 1 CorfuTable and 1 Thread doing putTable and getTable operations.
     * @throws IOException
     * @throws InterruptedException
     */
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

    /**
     * This funtion create given number tables and given number threads and
     * start the Thread to do putTable and getTable operations.
     * @param numThreads Number of threads.
     * @param numTables Number of tables.
     * @throws InterruptedException
     */
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

    /**
     * Performance test for having 10 threads doing putTable and getTable operations for
     * a single CorfuTable.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void CorfuTable10Thread1TableTest () throws IOException, InterruptedException {
        setMetricsReportFlags("corfutable-10-1");
        Process server = runServer();
        runtime = initRuntime();
        runTableOps(1, 10);
        killServer(server);
    }

    /**
     * Performance test for having 10 threads doing putTable and getTable operations for
     * 10 CorfuTables. In this test, threads don't share tables.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void CorfuTable10Thread10Table() throws IOException, InterruptedException {
        setMetricsReportFlags("corfutable-10-10");
        Process server = runServer();
        runtime = initRuntime();
        runTableOps(10, 10);
        killServer(server);
    }
}
