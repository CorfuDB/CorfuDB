package org.corfudb.benchmark;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;

import java.util.UUID;

/**
 * BenchmarkTest to CorfuTable.
 */
@Slf4j
public class CorfuTableBenchmarkTest extends BenchmarkTest {
    double ratio;
    String operationName;

    CorfuTableBenchmarkTest(ParseArgs parseArgs) {
        super(parseArgs);
        ratio = parseArgs.getRatio();
        operationName = parseArgs.getOp();
    }

    private void runProducer() {
        for (int i = 0; i < numThreads; i++) {
            CorfuRuntime runtime = runtimes.getRuntime(i);
            UUID uuid = streams.getStreamID(i);

            CorfuTable<String, String> table = corfuTables.getTable(uuid);
            CorfuTableOperations corfuTableOperations = new CorfuTableOperations(operationName, runtime, table, numRequests, ratio);
            runProducer(corfuTableOperations);
        }
    }

    private void runTest () {
        runProducer();
        runConsumers();
        waitForAppToFinish();
    }

    public static void main(String[] args) {
        ParseArgs parseArgs = new ParseArgs(args);
        CorfuTableBenchmarkTest corfuTableBenchmarkTest = new CorfuTableBenchmarkTest(parseArgs);
        corfuTableBenchmarkTest.runTest();
    }

}
