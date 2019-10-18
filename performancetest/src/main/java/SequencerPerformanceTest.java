package org.corfudb.performancetest;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Slf4j
public class SequencerPerformanceTest {
    public static String endPoint = "localhost:9000";
    public static int metricsPort = 1000;
    public static long seed = 1024;
    public static int randomBoundary = 100;
    public static long time = 1000;
    public static int milliToSecond = 1000;
    private CorfuRuntime runtime;
    private Random random;
    public static final Properties PROPERTIES = new Properties();

    public SequencerPerformanceTest() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("PerformanceTest.properties");

        try {
            PROPERTIES.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() {
        loadProperties();

        runtime = initRuntime();
        random = new Random(seed);
    }

    private void loadProperties() {
        if (PROPERTIES.contains("metricsPort")) {
            metricsPort = (int) PROPERTIES.get("metricsPort");
        }
        if (PROPERTIES.contains("endPoint")) {
            endPoint = (String) PROPERTIES.get("endPoint");
        }
        if (PROPERTIES.contains("seed")) {
            seed = (long) PROPERTIES.get("seed");
        }
        if (PROPERTIES.contains("randomBoundary")) {
            randomBoundary = (int) PROPERTIES.get("randomBoundary");
        }
        if (PROPERTIES.contains("time")) {
            time = (long) PROPERTIES.get("time");
        }
        if (PROPERTIES.contains("milliToSecond")) {
            milliToSecond = (int) PROPERTIES.get("milliToSecond");
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
            if ((System.currentTimeMillis() - start) / milliToSecond > time) {
                System.out.println("finish test");
                break;
            }
            tokenQuery(runtime, random.nextInt(randomBoundary));
            tokenTx(runtime, random.nextInt(randomBoundary));
        }
    }
}
