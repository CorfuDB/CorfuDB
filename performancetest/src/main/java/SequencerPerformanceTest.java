package org.corfudb.performancetest;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@Slf4j
public class SequencerPerformanceTest {
    public final static String DEFAULT_ENDPOINT = "localhost:9000";
    public final static int METRICS_PORT = 1000;
    public final static long SEED = 1024;
    public final static int RANDOM_BOUNDARY = 100;
    public final static long TIME = 1000;
    public final static int MILLI_TO_SECOND = 1000;
    private CorfuRuntime runtime;
    private Random random;

    @Before
    public void before() {
        runtime = initRuntime();
        random = new Random(SEED);
    }

    private CorfuRuntime initRuntime() {
        CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
        parameters.setPrometheusMetricsPort(METRICS_PORT);
        CorfuRuntime corfuRuntime = CorfuRuntime.fromParameters(parameters);
        corfuRuntime.addLayoutServer(DEFAULT_ENDPOINT);
        corfuRuntime.connect();
        return corfuRuntime;
    }

    private void tokenQuery(CorfuRuntime corfuRuntime, int numRequest) {
        System.out.println("*************tokenQuery**********" + numRequest);
        for (int i = 0; i < numRequest; i++) {
            long start = System.currentTimeMillis();
            TokenResponse tokenResponse = corfuRuntime.getSequencerView().query();
            System.out.println("token query latency: " + (System.currentTimeMillis() - start));
        }
        System.out.println("************tokenQuery end***********");
    }

    private void tokenTx(CorfuRuntime corfuRuntime, int numRequest) {
        System.out.println("*************token TX**********" + numRequest);
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
            long start = System.currentTimeMillis();
            corfuRuntime.getSequencerView().next(conflictInfo, stream);
            System.out.println("token tx latency: " + (System.currentTimeMillis() - start));
        }
        System.out.println("************token tx end***********");
    }

    @Test
    public void sequencerPerformanceTest() {
        long start = System.currentTimeMillis();
        while (true) {
            if ((System.currentTimeMillis() - start) / MILLI_TO_SECOND >  TIME) {
                System.out.println("finish test");
                break;
            }
            tokenQuery(runtime, random.nextInt(RANDOM_BOUNDARY));
            tokenTx(runtime, random.nextInt(RANDOM_BOUNDARY));
        }
    }
}
