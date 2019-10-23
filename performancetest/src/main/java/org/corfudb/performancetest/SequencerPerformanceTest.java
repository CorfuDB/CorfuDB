package org.corfudb.performancetest;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by Lin Dong on 10/18/19.
 */

@Slf4j
public class SequencerPerformanceTest {
    private String endPoint = "localhost:9000";
    private int metricsPort = 1000;
    private long seed = 1024;
    private int randomBoundary = 100;
    private long time = 1000;
    private int milliToSecond = 1000;
    private CorfuRuntime runtime;
    private Random random;
    private static final Properties PROPERTIES = new Properties();

    public SequencerPerformanceTest() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("PerformanceTest.properties");

        try {
            PROPERTIES.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        loadProperties();

        runtime = initRuntime();
        random = new Random(seed);
    }

    private void loadProperties() {
        if (PROPERTIES.containsKey("sequencerMetricsPort")) {
            metricsPort = Integer.parseInt((String)PROPERTIES.get("sequencerMetricsPort"));
        }
        if (PROPERTIES.containsKey("endPoint")) {
            endPoint = (String) PROPERTIES.get("endPoint");
        }
        if (PROPERTIES.containsKey("sequencerSeed")) {
            seed = Long.parseLong((String)PROPERTIES.get("sequencerSeed"));
        }
        if (PROPERTIES.containsKey("sequencerRandomBoundary")) {
            randomBoundary = Integer.parseInt((String) PROPERTIES.get("sequencerRandomBoundary"));
        }
        if (PROPERTIES.containsKey("sequencerTime")) {
            time = Long.parseLong((String) PROPERTIES.get("sequencerTime"));
        }
        if (PROPERTIES.containsKey("milliToSecond")) {
            milliToSecond = Integer.parseInt((String)PROPERTIES.get("milliToSecond"));
        }
    }

    private CorfuRuntime initRuntime() {
        CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
        parameters.setPrometheusMetricsPort(metricsPort);
        CorfuRuntime corfuRuntime = CorfuRuntime.fromParameters(parameters);
        corfuRuntime.addLayoutServer(endPoint);
        corfuRuntime.connect();
        return corfuRuntime;
    }

    private void tokenQuery(CorfuRuntime corfuRuntime, int numRequest) {
        for (int i = 0; i < numRequest; i++) {
            corfuRuntime.getSequencerView().query();
        }
    }

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

    @Test
    public void sequencerPerformanceTest() {
        long start = System.currentTimeMillis();
        while (true) {
            if ((System.currentTimeMillis() - start) / milliToSecond > time) {
                break;
            }
            tokenQuery(runtime, random.nextInt(randomBoundary));
            tokenTx(runtime, random.nextInt(randomBoundary));
        }
    }
}
