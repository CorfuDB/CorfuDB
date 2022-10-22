package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test exercises the transaction stream functionality. It starts multiple writers
 * that write transactions to separate tables. Every writer will write a consecutive range
 * of integers. To make sure that all updates have been produce and consumed, another polling
 * thread is started and computes a counters map. That map contains each observed stream and the
 * sum of updates it has seen for that stream.
 */

public class TransactionStreamIT extends AbstractIT {

    /**
     *
     * Extract the updates from the MultiObjectSMREntry and updates the counters map
     */
    private void ConsumeDelta(Map<UUID, Integer> map, List<ILogData> deltas, CorfuRuntime rt) {
        for (ILogData ld : deltas) {
            MultiObjectSMREntry multiObjSmr = (MultiObjectSMREntry) ld.getPayload(rt);
            for (Map.Entry<UUID, MultiSMREntry> multiSMREntry : multiObjSmr.getEntryMap().entrySet()) {
                for (SMREntry update : multiSMREntry.getValue().getUpdates()) {
                    int key = (int) update.getSMRArguments()[0];
                    int val = (int) update.getSMRArguments()[1];
                    assertThat(key).isEqualTo(val);
                    int newVal = map.getOrDefault(multiSMREntry.getKey(), 0) + key;
                    map.put(multiSMREntry.getKey(), newVal);
                }
            }
        }
    }

    @Test
    public void txnStreamTest() throws Exception {
        UUID txnStreamTag = CorfuRuntime.getStreamID("txn_stream_tag");

        Process server_1 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        final int runtimeCacheSize = 5;
        final int numWriters = 4;
        final int numWritesPerThread = 500;
        final long pollPeriodMs = 50;
        final long timeout = 30;

        ExecutorService consumer = Executors.newSingleThreadExecutor();
        List<CorfuRuntime> consumerRts = new ArrayList<>();

        // A thread that starts and consumes transaction updates via the Transaction Stream.
        Future<Map<UUID, Integer>> consumerState = consumer.submit(() -> {

            CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .maxCacheEntries(runtimeCacheSize)
                    .build();

            CorfuRuntime consumerRt = CorfuRuntime.fromParameters(params)
                    .parseConfigurationString(DEFAULT_ENDPOINT)
                    .connect();

            consumerRts.add(consumerRt);

            IStreamView txStream = consumerRt.getStreamsView().get(txnStreamTag);

            Map<UUID, Integer> counters = new HashMap<>(numWriters);
            int consumed = 0;

            // Stop polling only when all updates (from all writers) have
            // been consumed.
            while (consumed < numWriters * numWritesPerThread) {
                List<ILogData> entries = txStream.remaining();

                if (!entries.isEmpty()) {
                    ConsumeDelta(counters, entries, consumerRt);
                }

                consumed += entries.size();
                TimeUnit.MILLISECONDS.sleep(pollPeriodMs);
            }

            return counters;
        });


        ExecutorService producers = Executors.newFixedThreadPool(numWriters);

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .maxCacheEntries(runtimeCacheSize)
                .build();

        CorfuRuntime producersRt = CorfuRuntime.fromParameters(params)
                .parseConfigurationString(DEFAULT_ENDPOINT)
                .connect();

        // Spawn writers, where each thread creates a table and starts
        // writing non-conflicting transactions to that table. It will
        // write a range of consecutive numbers, each in a transaction.
        for (int x = 1; x <= numWriters; x++) {
            final int idx = x;
            producers.submit(() -> {
                PersistentCorfuTable<Integer, Integer> map = producersRt.getObjectsView()
                        .build()
                        .setStreamName(String.valueOf(idx))
                        .setStreamTags(txnStreamTag)
                        .setTypeToken(new TypeToken<PersistentCorfuTable<Integer, Integer>>() {})
                        .open();
                for (int i = 1; i <= numWritesPerThread; i++) {
                    producersRt.getObjectsView().TXBegin();
                    map.insert(i, i);
                    producersRt.getObjectsView().TXEnd();
                }
            });
        }

        producers.shutdown();
        consumer.shutdown();

        Map<UUID, Integer> counters = consumerState.get(timeout, TimeUnit.SECONDS);
        assertThat(counters).hasSize(numWriters);
        // Since we know the size of the maps and the written ranges, we can verify that
        // all updates have been received on the pollers end by computing the sum of all
        // the writes. The total sum of writes between [1, n] is n * (n + 1) / 2.
        int sumOfWritesPerTable = (numWritesPerThread * (numWritesPerThread + 1)) / 2;
        for (int x = 1; x <= numWriters; x++) {
            assertThat(counters.get(CorfuRuntime.getStreamID(String.valueOf(x)))).isEqualTo(sumOfWritesPerTable);

            PersistentCorfuTable<Integer, Integer> map = producersRt.getObjectsView()
                    .build()
                    .setStreamName(String.valueOf(x))
                    .setTypeToken(new TypeToken<PersistentCorfuTable<Integer, Integer>>() {})
                    .open();
            assertThat(map.size()).isEqualTo(numWritesPerThread);
        }

        producersRt.shutdown();
        consumerRts.forEach(CorfuRuntime::shutdown);
        shutdownCorfuServer(server_1);
    }
}
