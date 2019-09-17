package org.corfudb.benchmark;

import org.corfudb.runtime.CorfuRuntime;

public class CorfuTableBenchmarkTest extends BenchmarkTest {
    double ratio;
    String operationName = null;

    CorfuTableBenchmarkTest(ParseArgs parseArgs) {
        super(parseArgs);
        ratio = parseArgs.getRatio();
        operationName = parseArgs.getOp();
    }

    private void runProducer() {
        for (int i = 0; i < numThreads; i++) {
            CorfuRuntime rt = rts[i % rts.length];
            CorfuTableOperations corfuTableOperations = new CorfuTableOperations(operationName, rt, numRequests, ratio,false, false);
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
