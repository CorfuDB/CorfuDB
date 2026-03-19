package org.corfudb.integration;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.test.SampleSchema.SampleTableAMsg;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Test;

import org.awaitility.Awaitility;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for the silent trim gap fix, exercising the
 * DeltaStream / StreamingManager / CorfuStore subscription path.
 *
 * Scenario: write entries, subscribe and consume them, prefix trim past
 * the subscription point, restart the server, then re-subscribe at the
 * old position. The subscription or the first poll must raise a
 * StreamingException wrapping TrimmedException.
 */
@Slf4j
@SuppressWarnings("PMD.ClassNamingConventions")
public class StreamingSilentTrimGapIT extends AbstractIT {

    private static final String NAMESPACE = "test_namespace";
    private static final String TABLE_NAME = "trim_gap_table";
    private static final String STREAM_TAG = "sample_streamer_1";

    private static class ErrorCapturingListener implements StreamListener {
        @Getter
        private final LinkedList<CorfuStreamEntries> updates = new LinkedList<>();
        @Getter
        private volatile Throwable error;
        private final CountDownLatch errorLatch;
        private final CountDownLatch updateLatch;

        ErrorCapturingListener(CountDownLatch errorLatch, CountDownLatch updateLatch) {
            this.errorLatch = errorLatch;
            this.updateLatch = updateLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            updates.add(results);
            if (updateLatch != null) {
                updateLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            this.error = throwable;
            if (errorLatch != null) {
                errorLatch.countDown();
            }
        }
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testStreamingDetectsTrimGapAfterRestart() throws Exception {
        final String host = DEFAULT_HOST;
        final int port = DEFAULT_PORT;
        final String endpoint = host + ":" + port;

        Process corfuServer = new CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();

        CorfuRuntime runtime = createRuntime(endpoint);
        CorfuStore store = new CorfuStore(runtime);

        try {
            Table<Uuid, SampleTableAMsg, Uuid> table = store.openTable(
                    NAMESPACE, TABLE_NAME,
                    Uuid.class, SampleTableAMsg.class, Uuid.class,
                    TableOptions.fromProtoSchema(SampleTableAMsg.class)
            );

            // Phase 1: Write entries and subscribe to consume them.
            // Subscribe from before the writes so DeltaStream delivers them.
            Timestamp beforeWrites = Timestamp.newBuilder()
                    .setEpoch(0L)
                    .setSequence(Address.NON_ADDRESS)
                    .build();

            final int numUpdates = 10;
            for (int i = 0; i < numUpdates; i++) {
                try (TxnContext tx = store.txn(NAMESPACE)) {
                    Uuid key = Uuid.newBuilder().setMsb(i).setLsb(i).build();
                    SampleTableAMsg val = SampleTableAMsg.newBuilder()
                            .setPayload("val" + i).build();
                    tx.putRecord(table, key, val, key);
                    tx.commit();
                }
            }

            CountDownLatch updateLatch = new CountDownLatch(numUpdates);
            ErrorCapturingListener listener1 = new ErrorCapturingListener(null, updateLatch);
            store.subscribeListener(listener1, NAMESPACE, STREAM_TAG,
                    Collections.singletonList(TABLE_NAME), beforeWrites);

            assertThat(updateLatch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(listener1.getUpdates().size()).isEqualTo(numUpdates);

            long lastSeenSequence = listener1.getUpdates().getLast()
                    .getTimestamp().getSequence();

            store.unsubscribeListener(listener1);

            // Phase 2: Write more entries then prefix trim past what listener1 saw
            for (int i = numUpdates; i < numUpdates * 2; i++) {
                try (TxnContext tx = store.txn(NAMESPACE)) {
                    Uuid key = Uuid.newBuilder().setMsb(i).setLsb(i).build();
                    SampleTableAMsg val = SampleTableAMsg.newBuilder()
                            .setPayload("val" + i).build();
                    tx.putRecord(table, key, val, key);
                    tx.commit();
                }
            }

            runtime.getAddressSpaceView().prefixTrim(
                    runtime.getSequencerView().query().getToken());

            // Write one more entry above the trim so the stream has
            // surviving data in the log after restart.
            try (TxnContext tx = store.txn(NAMESPACE)) {
                Uuid key = Uuid.newBuilder().setMsb(999).setLsb(999).build();
                SampleTableAMsg val = SampleTableAMsg.newBuilder()
                        .setPayload("post-trim").build();
                tx.putRecord(table, key, val, key);
                tx.commit();
            }

            // Phase 3: Restart the server
            runtime.getLayoutView().getRuntimeLayout()
                    .getBaseClient(endpoint).restart().join();

            // Poll until the server is responsive after restart
            Awaitility.await().atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(() -> {
                        try {
                            runtime.invalidateLayout();
                            runtime.getLayoutView().getLayout();
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    });

            // Phase 4: Re-subscribe at the old position (lastSeenSequence).
            // With both fixes applied (trim() + initializeLogMetadata),
            // the restarted log unit sends correct per-stream trim marks
            // to the sequencer during bootstrap. The subscription should
            // either throw immediately in validateSyncAddress() or the
            // DeltaStream should detect the gap on the first poll and
            // propagate onError with a StreamingException/TrimmedException.

            Timestamp oldTs = Timestamp.newBuilder()
                    .setEpoch(0L)
                    .setSequence(lastSeenSequence)
                    .build();

            // Re-open the table (fresh runtime state after restart)
            store.openTable(NAMESPACE, TABLE_NAME,
                    Uuid.class, SampleTableAMsg.class, Uuid.class,
                    TableOptions.fromProtoSchema(SampleTableAMsg.class));

            CountDownLatch errorLatch = new CountDownLatch(1);
            ErrorCapturingListener listener2 = new ErrorCapturingListener(errorLatch, null);

            try {
                store.subscribeListener(listener2, NAMESPACE, STREAM_TAG,
                        Collections.singletonList(TABLE_NAME), oldTs);

                // If subscribe didn't throw, the error should arrive via onError
                // within a few poll cycles.
                assertThat(errorLatch.await(30, TimeUnit.SECONDS))
                        .as("onError must be called with a trim exception")
                        .isTrue();

                assertThat(listener2.getError())
                        .isNotNull()
                        .isInstanceOfAny(StreamingException.class, TrimmedException.class);

            } catch (StreamingException e) {
                assertThat(e.getCause()).isInstanceOf(TrimmedException.class);
            }

            // No post-gap data should have been delivered
            assertThat(listener2.getUpdates())
                    .as("no data should be delivered after the trim gap")
                    .isEmpty();
        } finally {
            runtime.shutdown();
            shutdownCorfuServer(corfuServer);
        }
    }
}
