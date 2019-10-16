package org.corfudb.benchmark;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * BenchmarkTest to CorfuTable.
 */
@Slf4j
public class CorfuTableBenchmarkTest extends BenchmarkTest {
    private final int numStreams;
    private final int numRequests;
    private final double ratio;
    private final String operationName;
    private final int keyNum;
    private final int valueSize;
    protected Streams streams;
    protected CorfuTables corfuTables;
    boolean shareTable;
    private List<Thread> threads;
    String endpoint;

    CorfuTableBenchmarkTest(CorfuTableParseArgs parseArgs) {
        super(parseArgs);
        ratio = parseArgs.getRatio();
        operationName = parseArgs.getOp();
        keyNum = parseArgs.getKeyNum();
        valueSize = parseArgs.getValueSize();
        numStreams = parseArgs.getNumStreams();
        numRequests = parseArgs.getNumRequests();
        streams = new Streams(numStreams);
        corfuTables = new CorfuTables(numStreams, openObjects());
        threads = new ArrayList<>();
        shareTable = parseArgs.getShareTable() == 0;
        endpoint = parseArgs.getEndpoint ();
    }

    private HashMap<UUID, CorfuTable> openObjects() {
        HashMap<UUID, CorfuTable> tempMaps = new HashMap<>();
        for (int i = 0; i < numStreams; i++) {
            CorfuRuntime runtime = runtimes.getRuntime(i);
            UUID uuid = streams.getStreamID(i);
            CorfuTable<String, String> map = runtime.getObjectsView()
                    .build()
                    .setStreamID(uuid)
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .open();

            tempMaps.put(uuid, map);
        }
        return tempMaps;
    }

    private void runProducer() {
        for (int i = 0; i < numThreads; i++) {
            CorfuRuntime runtime = runtimes.getRuntime(i);
            UUID uuid = streams.getStreamID(i);

            CorfuTable<String, String> table = corfuTables.getTable(uuid);
            CorfuTableOperations corfuTableOperations = new CorfuTableOperations(operationName, runtime, table, numRequests, ratio, keyNum, valueSize);
            if (shareTable) {
                runProducer(corfuTableOperations);
            } else {
                Thread thread = new Thread(() -> {
                    try {
                        long start = System.currentTimeMillis();
                        corfuTableOperations.execute();
                        long latency = System.currentTimeMillis() - start;
                        log.info("operation latency(milliseconds): " + latency);
                    } catch (Exception e) {
                        log.error("Operation failed with", e);
                    }
                });
                thread.start();
                threads.add(thread);
            }
        }
    }

    private void generateData() {
        runProducer();
        if (shareTable) {
            runConsumers();
            //waitForAppToFinish();
        } else {
            try {
                threads.get(0).join();
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
        }
    }

    private void runTest () {
        generateData ();

        CheckpointWrapper cpWrapper = new CheckpointWrapper(super.getRuntimes().getRuntime (0), streams.getAllMapNames (), corfuTables.getMaps ());
        log.info("First Trim operation latency(milliseconds): ");
        cpWrapper.trimAndCheckpoint ();

        generateData ();
        waitForAppToFinish();

        CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
        CorfuRuntime runtime = CorfuRuntime.fromParameters(parameters);
        runtime.addLayoutServer(endpoint);
        runtime.connect();

        //FastLoaderWrapper fastloader = new FastLoaderWrapper ();
        Thread thread = new Thread(() -> {
            try {
                long start = System.currentTimeMillis();
                FastLoaderWrapper.executeFastLoader(runtime, streams.getStreams ());
                long latency = System.currentTimeMillis() - start;
                log.info("operation latency(milliseconds): " + latency);
            } catch (Exception e) {
                log.error("Operation failed with", e);
            }
        });
        thread.start();

        Thread threadCP = new Thread(() -> {
            try {
                long start = System.currentTimeMillis();
                log.info("Second Trim operation latency(milliseconds)");
                cpWrapper.trimAndCheckpoint ();
                log.info("Third Trim operation latency(milliseconds): ");
                cpWrapper.trimAndCheckpoint ();
                long latency = System.currentTimeMillis() - start;
                log.info("Done Trim operation latency(milliseconds): " + latency);
            } catch (Exception e) {
                log.error("Operation failed with", e);
            }
        });
        threadCP.start();

        try {
            thread.join();
            threadCP.join();
        } catch (InterruptedException e) {
            e.printStackTrace ();
        }


    }

    public static void main(String[] args) {
        CorfuTableParseArgs parseArgs = new CorfuTableParseArgs(args);
        CorfuTableBenchmarkTest corfuTableBenchmarkTest = new CorfuTableBenchmarkTest(parseArgs);
        corfuTableBenchmarkTest.runTest();
    }
}