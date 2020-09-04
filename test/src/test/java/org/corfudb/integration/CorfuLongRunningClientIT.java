package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * This suite of tests exercises different access patterns for long running clients.
 * We consider a long running client any client who has accessed the object at an early state,
 * and accesses later when the state of the object has been modified by other clients (including checkpointer).
 */
public class CorfuLongRunningClientIT extends AbstractIT {

    private static final String streamName = "streamA";
    private static final int THREAD_COUNT = 10;

    private Process corfuServer;

    private CorfuRuntime client1;
    private CorfuRuntime cpClient;

    private CorfuTable<String, Integer> mapAClient1;
    private CorfuTable<String, Integer> mapAClientCP;

    private final int cpCycles = 4;

    @Test
    public void testSingleLongRunningClient() throws Exception {
        try {
            testLongRunningClient();

            // Single Thread access from Client 1 (long running client)
            assertThat(mapAClient1.get("KeyClient2")).isEqualTo(2);
        } finally {
            corfuServer.destroy();
        }
    }

    /**
     * Test the case where a long running client attempts to access (sync) the
     * object from multiple threads, and all accesses are non-transactional.
     *
     * @throws Exception
     */
    @Test
    public void testMultiThreadedLongRunningClientNonTxAccess() throws Exception {

        AtomicInteger totalExceptions = new AtomicInteger(0);

        try {
            testLongRunningClient();

            // Multi-thread access from Client 1 (long running client)
            Runnable nonTransactionalAccess = () -> {
                Integer value = mapAClient1.get("KeyClient2");
                if (value == null) {
                    totalExceptions.incrementAndGet();
                }
                assertThat(value).isEqualTo(2);
            };

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

            for (int i=0; i<THREAD_COUNT; i++) {
                executor.submit(nonTransactionalAccess);
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            assertThat(totalExceptions.get()).isEqualTo(0);

        } finally {
            client1.shutdown();
            corfuServer.destroy();
        }
    }

    /**
     * Test the case where a long running client attempts to access (sync) the
     * object from multiple threads, and all accesses are transactional.
     *
     * @throws Exception
     */
    @Test
    public void testMultiThreadedLongRunningClientTxAccess() throws Exception {

        AtomicInteger totalExceptions = new AtomicInteger(0);

        try {
            testLongRunningClient();

            // Multi-thread access from Client 1 (long running client)
            Runnable transactionalAccess = () -> {
                client1.getObjectsView().TXBegin();
                Integer value = mapAClient1.get("KeyClient2");
                client1.getObjectsView().TXBegin();
                if (value == null) {
                    totalExceptions.incrementAndGet();
                }
                assertThat(value).isEqualTo(2);
            };

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

            for (int i=0; i<THREAD_COUNT; i++) {
                executor.submit(transactionalAccess);
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            assertThat(totalExceptions.get()).isEqualTo(0);

        } finally {
            client1.shutdown();
            corfuServer.destroy();
        }
    }

    /**
     * Test the case where a long running client attempts to access (sync) the
     * object from multiple threads, and accesses are transactional and non-transactional.
     *
     * @throws Exception
     */
    @Test
    public void testMultiThreadedLongRunningClientMixedTxNonTxAccess() throws Exception {

        AtomicInteger totalExceptions = new AtomicInteger(0);

        try {
            testLongRunningClient();

            // Multi-thread access from Client 1 (long running client)
            Runnable transactionalAccess = () -> {
                client1.getObjectsView().TXBegin();
                Integer value = mapAClient1.get("KeyClient2");
                client1.getObjectsView().TXEnd();
                if (value == null) {
                    totalExceptions.incrementAndGet();
                }
                assertThat(value).isEqualTo(2);
            };

            Runnable nonTransactionalAccess = () -> {
                Integer value = mapAClient1.get("KeyClient2");
                if (value == null) {
                    totalExceptions.incrementAndGet();
                }
                assertThat(value).isEqualTo(2);
            };

            List<Runnable> typesOfAccess = new ArrayList<>();
            typesOfAccess.add(transactionalAccess);
            typesOfAccess.add(nonTransactionalAccess);

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

            for (int i=0; i<THREAD_COUNT; i++) {
                executor.submit(typesOfAccess.get(i % 2));
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            assertThat(totalExceptions.get()).isEqualTo(0);

        } finally {
            client1.shutdown();
            corfuServer.destroy();
        }
    }

    /**
     * The sequence of events that this test emulates are the following:
     *
     * (1) Start Corfu Server
     * (2) Start 3 Clients:
     *       - Client 1: long running client (one that writes at an early state and accesses after the object has been
     *            modified by other clients.
     *       - Client 2: client which modifies the state of the object for the long running client
     *       - cpClient: client performing checkpoint
     * (3) Client 1 (long running client): write first entry to mapA (token 0), globalPointer=0
     * (4) First Checkpoint Cycle (tokens: 1, 2, 3, 4), where 1 is the enforced hole and 2, 3, 4 checkpoint entries
     * (5) Client 2: writes second entry to mapA (token 5), globalPointer=5
     * (6) Run 4 cycles of Checkpoints (tokens: 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)
     * (7) Trim at the second checkpoint cycle @6, this will guarantee that the long running client won't be able
     *     to observe Client's 2 update (address 5) unless it loads from a checkpoint.
     *
     * @throws Exception
     */
    private void testLongRunningClient() throws Exception {

        CorfuRuntime client2 = null;

        try {
            CorfuTable<String, Integer> mapAClient2;

            // Start Corfu Server
            corfuServer = runServer(DEFAULT_PORT, true);

            // Setup Runtime's for 3 clients
            CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build();

            client1 = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            client1.parseConfigurationString(DEFAULT_HOST + ":" + DEFAULT_PORT);
            client1.connect();

            client2 = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            client2.parseConfigurationString(DEFAULT_HOST + ":" + DEFAULT_PORT);
            client2.connect();

            cpClient = CorfuRuntime.fromParameters(params);
            cpClient.parseConfigurationString(DEFAULT_HOST + ":" + DEFAULT_PORT);
            cpClient.connect();

            // Open Map for Client 1
            mapAClient1 = client1.getObjectsView()
                    .build()
                    .setStreamName(streamName)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            // Open Map for Client CP
            mapAClientCP = cpClient.getObjectsView()
                    .build()
                    .setStreamName(streamName)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            // Write Client 1
            mapAClient1.put("KeyClient1", 1);

            // Run Checkpoint
            checkpoint();

            // Write Client 2
            // Open Map for Client 2
            mapAClient2 = client2.getObjectsView()
                    .build()
                    .setStreamName(streamName)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            mapAClient2.put("KeyClient2", 2);
            assertThat(mapAClient2.size()).isEqualTo(2);

            // Run 4 checkpoint cpCycles
            Token trimMark = null;
            for (int i = 0; i < cpCycles; i++) {
                Token tmp = checkpoint();
                if (i == 0) {
                    trimMark = tmp;
                }
            }

            // Trim right after third checkpoint cycle
            cpClient.getAddressSpaceView().prefixTrim(trimMark);
            cpClient.getAddressSpaceView().gc();
        } finally {
            client2.shutdown();
            cpClient.shutdown();
        }
    }

    private Token checkpoint() {
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(mapAClientCP);
        Token trimMark = mcw.appendCheckpoints(cpClient, "author");
        return trimMark;
    }
}
