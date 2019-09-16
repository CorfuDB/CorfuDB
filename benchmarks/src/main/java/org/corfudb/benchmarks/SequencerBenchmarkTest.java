package org.corfudb.benchmarks;

import org.corfudb.runtime.CorfuRuntime;

public class SequencerBenchmarkTest extends BenchmarkTest {
    SequencerBenchmarkTest(String[] args) {
        super(args);
    }

    private void runProducer(String operationName) {
        for (int i = 0; i < numThreads; i++) {
            CorfuRuntime rt = rts[i % rts.length];
            SequencerOperations sequencerOperations = new SequencerOperations(operationName, rt, numRequests);
            runTaskProducer(sequencerOperations);
        }
    }
    private void runTest(String operationName) {
        runProducer(operationName);
        runConsumers();
        waitForAppToFinish();
    }

    public static void main(String[] args) {
        SequencerBenchmarkTest sequencerBenchmarkTest = new SequencerBenchmarkTest(args);
        sequencerBenchmarkTest.runTest("query");
    }
}
