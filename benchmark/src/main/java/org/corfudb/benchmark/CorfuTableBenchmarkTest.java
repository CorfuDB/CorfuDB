package org.corfudb.benchmark;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;

import java.util.HashMap;
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
            runProducer(corfuTableOperations);
        }
    }

    private void runTest () {
        runProducer();
        runConsumers();
        waitForAppToFinish();
    }

    public static void main(String[] args) {
        CorfuTableParseArgs parseArgs = new CorfuTableParseArgs(args);
        CorfuTableBenchmarkTest corfuTableBenchmarkTest = new CorfuTableBenchmarkTest(parseArgs);
        corfuTableBenchmarkTest.runTest();
    }
}