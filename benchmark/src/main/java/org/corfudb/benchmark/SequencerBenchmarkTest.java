package org.corfudb.benchmark;

import org.corfudb.runtime.CorfuRuntime;

public class SequencerBenchmarkTest extends BenchmarkTest {
    String operationName = null;
    SequencerBenchmarkTest(ParseArgs parseArgs) {
        super(parseArgs);
        operationName = parseArgs.getOp();
//        System.out.println("num runtimes" + numRuntimes);
//        System.out.println("num requests" + numRequests);
//        System.out.println("num threads" + numThreads);
//        System.out.println("endpoint" + endpoint);
//        System.out.println("operation" + operationName);
    }

    private void runProducer() {
        for (int i = 0; i < numThreads; i++) {
            CorfuRuntime rt = rts[i % rts.length];
            SequencerOperations sequencerOperations = new SequencerOperations(operationName, rt, numRequests);
            runTaskProducer(sequencerOperations);
        }
    }
    private void runTest() {
        runProducer();
        runConsumers();
        waitForAppToFinish();
    }

    public static void main(String[] args) {
        ParseArgs parseArgs = new ParseArgs(args);
        SequencerBenchmarkTest sequencerBenchmarkTest = new SequencerBenchmarkTest(parseArgs);
        sequencerBenchmarkTest.runTest();
    }
}
