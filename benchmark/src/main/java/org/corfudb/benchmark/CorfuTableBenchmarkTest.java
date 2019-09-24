package org.corfudb.benchmark;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;

import java.util.UUID;

/**
 * BenchmarkTest to CorfuTable.
 */
@Slf4j
public class CorfuTableBenchmarkTest extends BenchmarkTest {
    double ratio;
    String operationName;
    int keySize;

    CorfuTableBenchmarkTest(ParseArgs parseArgs) {
        super(parseArgs);
        ratio = parseArgs.getRatio();
        operationName = parseArgs.getOp();
        keySize = parseArgs.getKeySize();
    }

    private void runProducer() {
        for (int i = 0; i < numThreads; i++) {
            CorfuRuntime runtime = runtimes.getRuntime(i);
            UUID uuid = streams.getStreamID(i);

            CorfuTable<String, String> table = corfuTables.getTable(uuid);
            CorfuTableOperations corfuTableOperations = new CorfuTableOperations(operationName, runtime, table, numRequests, ratio, keySize);
            runProducer(corfuTableOperations);
        }
    }

    private void runTest () {
        runProducer();
        runConsumers();
        waitForAppToFinish();
    }

//    @Test
//    public void testCorfuTableBuild(String[] args) {
//        ParseArgs parseArgs = new ParseArgs(args);
//        CorfuTableBenchmarkTest corfuTableBenchmarkTest = new CorfuTableBenchmarkTest(parseArgs);
//        runProducer();
//        runConsumers();
//        waitForAppToFinish();
//    }

    public static void main(String[] args) {
        ParseArgs parseArgs = new ParseArgs(args);
        CorfuTableBenchmarkTest corfuTableBenchmarkTest = new CorfuTableBenchmarkTest(parseArgs);
        corfuTableBenchmarkTest.runTest();

    }
}