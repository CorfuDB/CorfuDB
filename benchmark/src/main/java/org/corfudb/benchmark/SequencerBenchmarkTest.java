package org.corfudb.benchmark;

import org.corfudb.runtime.CorfuRuntime;

/**
 * This class is for Sequencer's benchmark tests.
 * operationName can be: query, raw, multistream, tx, getstreamaddr,
 * each name maps to a kind of token request and API in Sequencer Server.
 */
public class SequencerBenchmarkTest extends BenchmarkTest {
    String operationName = null;
    SequencerBenchmarkTest(ParseArgs parseArgs) {
        super(parseArgs);
        operationName = parseArgs.getOp();
    }

    private void runProducer() {
        for (int i = 0; i < numThreads; i++) {
            CorfuRuntime rt = runtimes.getRuntime(i);
            SequencerOperations sequencerOperations = new SequencerOperations(operationName, rt, numRequests);
            runProducer(sequencerOperations);
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