package org.corfudb.benchmarks;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.MetricsUtils;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class is the benchmark tests for sequencer server. It tests main APIs in SequencerClient.java
 * It tests TK_QUERY, TK_RAW, TK_MULTI_STREAM through nextToken API.
 * It tests TK_TX through nextToken API with conflictInfo.
 * It also tests getStreamsAddressSpace and bootstrap API.
 */
@Slf4j
public class SequencerBenchmarkTests extends BenchmarkTest {
    public SequencerBenchmarkTests(String[] args) {
        super(args);
        MetricsUtils.metricsReportingSetup(CorfuRuntime.getDefaultMetrics());
    }

    /**
     * This function tests the performance of nextToken API from SequencerClient.
     * It sends numThreads * numRequests times TK_QUERY requests through SequencerClient,
     * collect and report metrics to csv files.
     */
    private void tokenQueryBenchmarkTest() {
        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        for (int tNum = 0; tNum < numThreads; tNum++) {
            CorfuRuntime rt = rts[tNum % rts.length];
            service.submit(() -> {
                for (int reqId = 0; reqId < numRequests; reqId++) {
                    long start = System.nanoTime();
                    rt.getSequencerView().query();
                    // sequencerClient.nextToken(Collection.emptyList(), 0);
                    long latency = System.nanoTime() - start;
                    log.info("nextToken request latency: "+ latency);
                }
            });
        }

        service.shutdown();
        try {

            service.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.warn (e.toString());
        }
    }

    /**
     * This function tests the performance of nextToken API from SequencerClient.
     * It sends numThreads * numRequests times TK_RAW requests through SequencerClient,
     * collect and report metrics to csv files.
     */
    private void tokenRawBenchmarkTest() {
        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        for (int tNum = 0; tNum < numThreads; tNum++) {
            CorfuRuntime rt = rts[tNum % rts.length];
            service.submit(() -> {
                for (int reqId = 0; reqId < numRequests; reqId++) {
                    long start = System.nanoTime();
                    rt.getSequencerView().next();
                    // sequencerClient.nextToken(Collection.emptyList(), 1);
                    long latency = System.nanoTime() - start;
                    log.info("nextToken request latency: "+ latency);
                }
            });
        }

        service.shutdown();
        try {

            service.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.warn (e.toString());
        }
    }

    /**
     * This function tests the performance of nextToken API from SequencerClient.
     * It sends numThreads * numRequests times TK_MULTI_STREAM requests through SequencerClient,
     * collect and report metrics to csv files.
     */
    private void tokenMultiStreamBenchmarkTest() {
        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        for (int tNum = 0; tNum < numThreads; tNum++) {
            CorfuRuntime rt = rts[tNum % rts.length];
            UUID stream = UUID.nameUUIDFromBytes("Stream".getBytes());
            service.submit(() -> {
                for (int reqId = 0; reqId < numRequests; reqId++) {
                    long start = System.nanoTime();
                    rt.getSequencerView().next(stream);
                    // sequencerClient.nextToken(Collections.singletonList(stream), 1);
                    long latency = System.nanoTime() - start;
                    log.info("nextToken request latency: "+ latency);
                }
            });
        }

        service.shutdown();
        try {

            service.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.warn (e.toString());
        }
    }

    /**
     * This function tests the performance of nextToken API with conflictInfo from SequencerClient.
     * It sends numThreads * numRequests times TK_TX requests through SequencerClient,
     * collect and report metrics to csv files.
     */
    private void tokenTxnBenchmarksTest() {
        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        for (int tNum = 0; tNum < numThreads; tNum++) {
            CorfuRuntime rt = rts[tNum % rts.length];
            //SequencerClient sequencerClient = rt.getLayoutView().getRuntimeLayout().getPrimarySequencerClient();
            service.submit(() -> {
                for (int reqId = 0; reqId < numRequests; reqId++) {
                    long start = System.nanoTime();
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

                    rt.getSequencerView().next(conflictInfo, stream);
                    //sequencerClient.nextToken(Collections.singletonList(stream), 1, conflictInfo);
                    long latency = System.nanoTime() - start;
                    log.info("nextToken request latency: " + latency);
                }
            });
        }
        service.shutdown();
        try {
            service.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.warn(e.toString());
        }
    }

    /**
     * This function tests the performance of getStreamAddressSpace API from SequencerClient.
     * It sends numThreads * numRequests times getStreamAddressSpace requests through SequencerClient,
     * collect and report metrics to csv files.
     */
    private void getStreamsAddressSpaceBenchmarkTest() {
        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        for (int tNum = 0; tNum < numThreads; tNum++) {
            CorfuRuntime rt = rts[tNum % rts.length];
            UUID stream = UUID.nameUUIDFromBytes("Stream".getBytes());
            final int tokenCount = 3;
            //SequencerClient sequencerClient = rt.getLayoutView().getRuntimeLayout().getPrimarySequencerClient();
            service.submit(() -> {
                for (int reqId = 0; reqId < numRequests; reqId++) {
                    long start = System.nanoTime();
                    rt.getSequencerView().getStreamAddressSpace(new StreamAddressRange(stream,  tokenCount, Address.NON_ADDRESS));
                    //sequencerClient.getStreamsAddressSpace(Arrays.asList(new StreamAddressRange(stream,  tokenCount, Address.NON_ADDRESS)));
                    long latency = System.nanoTime() - start;
                    log.info("nextToken request latency: "+ latency);
                }
            });
        }
        service.shutdown();
        try {
            service.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.warn (e.toString());
        }
    }

    public static void main(String[] args) {
        SequencerBenchmarkTests sb = new SequencerBenchmarkTests(args);
        //sb.tokenQueryBenchmarkTest();
        //sb.tokenRawBenchmarkTest();
        sb.tokenMultiStreamBenchmarkTest();
        //sb.tokenTxnBenchmarksTest();
        //sb.getStreamsAddressSpaceBenchmarkTest();
    }
}
