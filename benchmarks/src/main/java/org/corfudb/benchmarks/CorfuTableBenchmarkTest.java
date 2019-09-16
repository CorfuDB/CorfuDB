package org.corfudb.benchmarks;

import org.corfudb.runtime.CorfuRuntime;

public class CorfuTableBenchmarkTest extends BenchmarkTest {
    CorfuTableBenchmarkTest(String[] args) {
        super(args);
    }

    private void runProducer(String operationName) {
        for (int i = 0; i < numThreads; i++) {
            CorfuRuntime rt = rts[i % rts.length];
            CorfuTableOperations corfuTableOperations = new CorfuTableOperations(operationName, rt, numRequests, false, false);
            runTaskProducer(corfuTableOperations);
        }
    }

    private void runTest (String operationName) {
        runProducer(operationName);
        runConsumers();
        waitForAppToFinish();
    }

    public static void main(String[] args) {
        CorfuTableBenchmarkTest corfuTableBenchmarkTest = new CorfuTableBenchmarkTest(args);
        corfuTableBenchmarkTest.runTest("put");
    }

}
