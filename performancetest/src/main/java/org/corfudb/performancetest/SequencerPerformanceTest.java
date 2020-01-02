package org.corfudb.performancetest;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;
import java.io.IOException;
import java.util.*;

/**
 * SequencerPerformanceTest is the sub class of PerformanceTest.
 * This class is mainly about PerformanceTest for sequencer. The operations
 * tested in this class is query and token request with conflictInfo.
 * @author Lin Dong
 */
@Slf4j
public class SequencerPerformanceTest extends PerformanceTest{
    /**
     * The boundary of the integer randomly generated.
     */
    private final int randomBoundary;
    /**
     * How long the test runs.
     */
    private final long time;
    /**
     * Random Object.
     */
    private final Random random;

    /**
     * Constructor. Initiate some variables.
     */
    public SequencerPerformanceTest() {
        long seed = Long.parseLong(PROPERTIES.getProperty("sequencerSeed", "1024"));
        randomBoundary = Integer.parseInt(PROPERTIES.getProperty("sequencerRandomBoundary", "100"));
        time = Long.parseLong(PROPERTIES.getProperty("sequencerTime", "100"));
        random = new Random(seed);
    }

    /**
     * tokenQuery calls query API of SequencerView.
     * @param corfuRuntime runtime.
     * @param numRequest how many times the function calls query().
     */
    private void tokenQuery(CorfuRuntime corfuRuntime, int numRequest) {
        for (int i = 0; i < numRequest; i++) {
            corfuRuntime.getSequencerView().query();
        }
    }

    /**
     * tokenTx calls next(conflictInfo, stream) API of SequencerView.
     * @param corfuRuntime runtime.
     * @param numRequest how many times the function calls next().
     */
    private void tokenTx(CorfuRuntime corfuRuntime, int numRequest) {
        for (int i = 0; i < numRequest; i++) {
            UUID transactionID = UUID.nameUUIDFromBytes("transaction".getBytes());
            UUID stream = UUID.randomUUID();
            Map<UUID, Set<byte[]>> conflictMap = new HashMap<>();
            Set<byte[]> conflictSet = new HashSet<>();
            byte[] value = new byte[]{0, 0, 0, 1};
            conflictSet.add(value);
            conflictMap.put(stream, conflictSet);

            Map<UUID, Set<byte[]>> writeConflictParams = new HashMap<>();
            Set<byte[]> writeConflictSet = new HashSet<>();
            byte[] value1 = new byte[]{0, 0, 0, 1};
            writeConflictSet.add(value1);
            writeConflictParams.put(stream, writeConflictSet);

            TxResolutionInfo conflictInfo = new TxResolutionInfo(transactionID,
                    new Token(0, -1),
                    conflictMap,
                    writeConflictParams);
            corfuRuntime.getSequencerView().next(conflictInfo, stream);
        }
    }

    /**
     * The performance test for sequencer.
     * This test starts a CorfuServer and a CorfuRuntime, continues sending query and
     * token requests for a period, then shuts down the CorfuServer.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void sequencerPerformanceTest() throws IOException, InterruptedException {
        setMetricsReportFlags("sequencer");
        Process server = runServer();
        runtime = initRuntime();
        long start = System.currentTimeMillis();
        while (true) {
            if ((System.currentTimeMillis() - start) / MILLI_TO_SECOND > time) {
                break;
            }
            tokenQuery(runtime, random.nextInt(randomBoundary));
            tokenTx(runtime, random.nextInt(randomBoundary));
        }
        killServer(server);
    }
}
