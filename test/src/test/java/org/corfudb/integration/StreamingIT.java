package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.IsolationLevel;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.corfudb.test.SampleSchema.SampleTableAMsg;
import org.corfudb.test.SampleSchema.SampleTableBMsg;
import org.corfudb.test.SampleSchema.SampleTableCMsg;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

/**
 * Simple test that inserts data into CorfuStore and tests Streaming.
 */
@Slf4j
public class    StreamingIT extends AbstractIT {

    private static String corfuSingleNodeHost;
    private static int corfuStringNodePort;
    private static String singleNodeEndpoint;
    private Process corfuServer;
    private CorfuStore store;

    private final String namespace = "test_namespace";
    private final String defaultTableName = "table_testA";
    private final String defaultTag = "sample_streamer_1";

    private final int sleepTime = 100;

    /**
     * A helper method that takes host and port specification, start a single server and
     * returns a process.
     */
    List<Process> processes = new CopyOnWriteArrayList<>();
    private Process runSinglePersistentServer(String host, int port) throws IOException {
        Process p = new AbstractIT.CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();
        processes.add(p);
        return p;
    }

    /**
     * Load properties for a single node corfu server before each test
     */
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.parseInt(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    @After
    public void cleanup() {
        processes.forEach(p -> {
            try {
                shutdownCorfuServer(p);
            } catch (Exception e) {
                log.error("clean up failed", e);
            }
        });
    }

    /**
     * A StreamListener implementation to be used in the tests.
     * This listener accumulates all updates streamed to it into a linked list that
     * can then be used to verify.
     * This listener can throw an exception on 'n' number of updates, in order to
     * trigger the onError(Throwable t) and consequently, automatically unsubscribe.
     */
    private static class StreamListenerImpl implements StreamListener {

        @Getter
        private final String name;

        private int updatesToError = 0;

        @Getter
        private Timestamp timestamp;

        @Getter
        private final LinkedList<CorfuStreamEntries> updates = new LinkedList<>();

        private CountDownLatch latch = null;
        private CountDownLatch updateLatch = null;

        StreamListenerImpl(String name) {
            this.name = name;
        }

        StreamListenerImpl(String name, CountDownLatch updateLatch) {
            this(name);
            this.updateLatch = updateLatch;
        }

        StreamListenerImpl(String name, int updatesToError, CountDownLatch latch) {
            this(name);
            this.updatesToError = updatesToError;
            this.latch = latch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            updates.add(results);
            timestamp = results.getTimestamp();
            if (updateLatch != null) {
                updateLatch.countDown();
            }
            if (updates.size() == updatesToError) {
                throw new IndexOutOfBoundsException("Artificial exception to trigger onError");
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (throwable instanceof IndexOutOfBoundsException) {
                latch.countDown();
            }
        }

        @Override
        public boolean equals(Object o) {
            return this.hashCode() == o.hashCode();
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * A StreamListener implementation to be used in the tests.
     * This listener blocks on a latch when executing onNext()
     */
    private static class BlockingStreamListener extends StreamListenerImpl {

        private final CountDownLatch latch;

        BlockingStreamListener(String name, CountDownLatch latch) {
            super(name);
            this.latch = latch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                fail();
            }
            super.updates.add(results);
        }
    }

    /**
     * Basic Streaming Test with a single table.
     * <p>
     * The test updates a single table a few times and ensures that the listeners subscribed
     * to the streaming updates from the table get the updates.
     * <p>
     * The test verifies the following:
     * 1. Streaming subscriptions with different timestamps get the right set of updates.
     * 2. The operation type, key, payload and metadata values are as expected.
     * 3. A listener does not get stream updates after it has been unsubscribed.
     * 3. Delete operations are indeed idempotent.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testStreamingSingleTable() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = Timestamp.newBuilder().setEpoch(0L).setSequence(Address.NON_ADDRESS).build();

        // Create a table.
        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                "test_namespace", "tableA",
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );

        // Make some updates to the table, more than the buffer size.
        final int bufferSize = 5;
        final int numUpdates = bufferSize * 2 + 1;
        for (int i = 0; i < numUpdates; i++) {
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            SampleTableAMsg msg = SampleTableAMsg.newBuilder().setPayload(String.valueOf(i)).build();
            TxnContext tx = store.txn("test_namespace");
            tx.putRecord(tableA, uuid, msg, uuid);
            tx.commit();
        }

        // Subscribe to streaming updates from the table using customized buffer size.
        StreamListenerImpl listener1 = new StreamListenerImpl("stream_listener_1");
        store.subscribeListener(listener1, "test_namespace", defaultTag,
                Collections.singletonList("tableA"), ts1, bufferSize);

        // After a brief wait verify that the listener gets all the updates.
        long sleepPeriod = runtime.getParameters().getStreamingPollPeriod().toMillis() * 3;
        TimeUnit.MILLISECONDS.sleep(sleepPeriod);

        LinkedList<CorfuStreamEntries> updates = listener1.getUpdates();
        assertThat(updates.size()).isEqualTo(numUpdates);

        for (int i = 0; i < numUpdates; i++) {
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            SampleTableAMsg msg = SampleTableAMsg.newBuilder().setPayload(String.valueOf(i)).build();
            CorfuStreamEntries update = updates.get(i);
            assertThat(update.getEntries()).hasSize(1);
            List<CorfuStreamEntry> entry = update.getEntries().values().stream().findFirst().get();
            assertThat(entry).hasSize(1);
            assertThat(entry.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
            assertThat(entry.get(0).getKey()).isEqualTo(uuid);
            assertThat(entry.get(0).getPayload()).isEqualTo(msg);
            assertThat(entry.get(0).getMetadata()).isEqualTo(uuid);
        }

        // Add another subscriber to the same table starting now.
        StreamListenerImpl listener2 = new StreamListenerImpl("stream_listener_2");
        store.subscribeListener(listener2, "test_namespace", defaultTag,
                Collections.singletonList("tableA"), null);

        TxnContext tx = store.txn("test_namespace");
        Uuid uuid0 = Uuid.newBuilder().setMsb(0).setLsb(0).build();
        tx.delete(tableA, uuid0);
        tx.delete(tableA, uuid0);
        tx.commit();

        // Now repeat the delete operation to check for idempotency
        tx = store.txn("test_namespace");
        tx.delete(tableA, uuid0);
        tx.commit();
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        // Both the listeners should see ONLY 1 delete operation.
        updates = listener1.getUpdates();
        CorfuStreamEntries update = updates.getLast();
        assertThat(update.getEntries()).hasSize(1);
        List<CorfuStreamEntry> entry = update.getEntries().values().stream().findFirst().get();
        assertThat(update.getTimestamp().getSequence()).isGreaterThan(0L);
        assertThat(entry).hasSize(1);
        assertThat(entry.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.DELETE);
        assertThat(entry.get(0).getKey()).isEqualTo(uuid0);
        assertThat(entry.get(0).getPayload()).isNull();
        assertThat(entry.get(0).getMetadata()).isNull();

        // Ensure that the listener2 sees only one update, i.e the last delete.
        updates = listener2.getUpdates();
        assertThat(updates).hasSize(1);
        update = updates.getLast();
        assertThat(update.getEntries()).hasSize(1);
        entry = update.getEntries().values().stream().findFirst().get();
        assertThat(entry).hasSize(1);
        assertThat(entry.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.DELETE);
        assertThat(entry.get(0).getKey()).isEqualTo(uuid0);
        assertThat(entry.get(0).getPayload()).isNull();
        assertThat(entry.get(0).getMetadata()).isNull();

        // Unsubscribe listener1 and ensure that it no longer gets any updates.
        store.unsubscribeListener(listener1);
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        tx = store.txn("test_namespace");
        Uuid uuid1 = Uuid.newBuilder().setMsb(1).setLsb(1).build();
        tx.clear(tableA);
        tx.commit();

        // Listener1 should see no new updates, where as listener2 should see the latest delete.
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        updates = listener1.getUpdates();
        assertThat(updates).hasSize(numUpdates + 1);
        updates = listener2.getUpdates();
        assertThat(updates).hasSize(2);
        update = updates.getLast();
        assertThat(update.getEntries()).hasSize(1);
        entry = update.getEntries().values().stream().findFirst().get();
        assertThat(entry).hasSize(1);
        assertThat(entry.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.CLEAR);
        assertThat(entry.get(0).getKey()).isNull();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Verify that empty string tags are not supported for subscribers.
     *
     * @throws Exception
     */
    @Test
    public void testStreamingSingleTableEmptyTag() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = Timestamp.newBuilder().setEpoch(0L).setSequence(Address.NON_ADDRESS).build();
        final String tableName = "tableNoTags";

        // Create a table.
        Table<Uuid, SampleTableCMsg, Uuid> tableNoTags = store.openTable(
                "test_namespace", tableName,
                Uuid.class, SampleTableCMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableCMsg.class)
        );

        // Make some updates to the table, more than the buffer size.
        final int bufferSize = 5;
        final int numUpdates = bufferSize * 2 + 1;
        for (int i = 0; i < numUpdates; i++) {
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            SampleTableCMsg msg = SampleTableCMsg.newBuilder().setPayload(String.valueOf(i)).build();
            TxnContext tx = store.txn("test_namespace");
            tx.putRecord(tableNoTags, uuid, msg, uuid);
            tx.commit();
        }

        // Subscribe to streaming updates from the table using customized buffer size.
        StreamListenerImpl listener1 = new StreamListenerImpl("stream_listener_1");
        assertThrows(IllegalArgumentException.class, () -> store.subscribeListener(listener1, "test_namespace",
                "", Collections.singletonList(tableName), ts1, bufferSize));

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Example to show how to retrieve the previous value from a streamed entry
     * @param <K>
     * @param <V>
     * @param <M>
     */
    class PrevValueStreamer<K extends Message, V extends Message, M extends Message> implements StreamListener {
        @Getter
        private int recordCount;
        private CorfuStore corfuStore;
        private final String namespace;
        private final String tableName;
        private final CountDownLatch latch;
        public PrevValueStreamer(CorfuStore corfuStore, String namespace, String tableName, CountDownLatch latch) {
            recordCount = 0;
            this.corfuStore = corfuStore;
            this.namespace = namespace;
            this.tableName = tableName;
            this.latch = latch;
        }

        public String toString() {
            return namespace+tableName;
        }
        /**
         * A corfu update can/may have multiple updates belonging to different streams.
         * This callback will return those updates as a list grouped by their Stream UUIDs.
         *
         * @param results is a map of stream UUID -> list of entries of this stream.
         */
        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((k, v) -> {
                v.forEach(entry -> {
                    if (recordCount > 0) {
                        final CorfuStoreEntry<K, V, M> previousRecord =
                                getPreviousRecord(namespace, tableName, entry, results.getTimestamp().getEpoch(),
                                        results.getTimestamp().getSequence());
                        SampleTableAMsg prevMsg = (SampleTableAMsg) previousRecord.getPayload();
                        if (recordCount > 0) {
                            assertThat(prevMsg.getPayload()).isEqualTo("val"+(recordCount - 1));
                        } else {
                            assertThat(previousRecord.getPayload()).isNull();
                        }
                    }
                });
            });
            recordCount++;
            latch.countDown();
        }

        /**
         *  Fetch the previous value of a record from the table using the sequence - 1 snapshot
         * @param namespace
         * @param tableName
         * @param entry
         * @param epoch
         * @param sequence
         * @return
         */
        public CorfuStoreEntry<K, V, M> getPreviousRecord(String namespace, String tableName,
                                                          CorfuStreamEntry<K, V, M> entry,
                                                          long epoch,
                                                          long sequence) {
            Timestamp prevTimestamp = Timestamp.newBuilder()
                    .setEpoch(epoch)
                    .setSequence(sequence - 1)
                    .build();
            CorfuStoreEntry<K,V,M> prevRecord;
            try (TxnContext tx = this.corfuStore.txn(namespace, IsolationLevel.snapshot(prevTimestamp))) {
                prevRecord = tx.getRecord(tableName, entry.getKey());
                tx.commit();
            }
            return prevRecord;
        }

        /**
         * Callback to indicate that an error or exception has occurred while streaming or that the stream is
         * shutting down. Some exceptions can be handled by restarting the stream (TrimmedException) while
         * some errors (SystemUnavailableError) are unrecoverable.
         *
         * @param throwable
         */
        @Override
        public void onError(Throwable throwable) {
            assertThat(throwable).isNull();
        }
    }

    /** Verify the same listener cannot be used across different subscribers
     *
     * @throws Exception
     */
    @Test
    public void testStreamingPrevValue() throws Exception {
        // Run a corfu server and start runtime
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntimeWithCache(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);
        String ns = "test_namespace";
        String tn = "tableA";

        // Create two tables.
        Table<Uuid, SampleTableAMsg, Uuid> table = store.openTable(ns, tn,
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class));

        final int numRecords = PARAMETERS.NUM_ITERATIONS_LOW;
        CountDownLatch latch = new CountDownLatch(numRecords);
        // Subscribe to streaming updates from tableA using listenerCommon
        PrevValueStreamer listenerCommon = new PrevValueStreamer<Uuid, SampleTableAMsg, Uuid>(store, ns, tn, latch);
        store.subscribeListener(listenerCommon, ns, defaultTag,
                Collections.singletonList(tn));
        for (int i = 0; i < numRecords; i++) {
            try (TxnContext tx = store.txn(namespace)) {
                Uuid key = Uuid.newBuilder().setLsb(0).setMsb(0).build();
                SampleTableAMsg val = SampleTableAMsg.newBuilder()
                        .setPayload("val"+i).build();
                tx.putRecord(table, key, val, key);
                tx.commit();
            }
        }

        // Await until the listener gets all the updates
        latch.await();
        assertThat(listenerCommon.getRecordCount()).isEqualTo(numRecords);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Streaming Test with two different tables with different stream tags.
     * Table A has tag1 and tag2, table B has tag2 and tag3.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testStreamingMultiTableStreams() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = Timestamp.newBuilder().setEpoch(0L).setSequence(Address.NON_ADDRESS).build();

        // Create 2 tables.
        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                "test_namespace", "tableA",
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );

        Table<Uuid, SampleTableBMsg, Uuid> tableB = store.openTable(
                "test_namespace", "tableB",
                Uuid.class, SampleTableBMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableBMsg.class)
        );

        // Make some updates to the tables.
        int index = 0;
        Uuid uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
        SampleTableAMsg msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
        TxnContext tx = store.txn("test_namespace");
        tx.putRecord(tableA, uuid, msgA, uuid);
        tx.commit();

        index++;
        uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
        SampleTableBMsg msgB = SampleTableBMsg.newBuilder().setPayload(String.valueOf(index)).build();
        tx = store.txn("test_namespace");
        tx.putRecord(tableB, uuid, msgB, uuid);
        tx.commit();

        index++;
        uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
        msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
        msgB = SampleTableBMsg.newBuilder().setPayload(String.valueOf(index)).build();
        tx = store.txn("test_namespace");
        tx.putRecord(tableA, uuid, msgA, uuid);
        tx.putRecord(tableB, uuid, msgB, uuid);
        tx.commit();

        // Subscribe to streaming updates.
        StreamListenerImpl listener1 = new StreamListenerImpl("stream_listener_1");
        StreamListenerImpl listener2 = new StreamListenerImpl("stream_listener_2");
        StreamListenerImpl listener3 = new StreamListenerImpl("stream_listener_3");
        store.subscribeListener(listener1, "test_namespace", "sample_streamer_1",
                Collections.singletonList("tableA"), ts1);
        store.subscribeListener(listener2, "test_namespace", "sample_streamer_2",
                Arrays.asList("tableA", "tableB"), ts1);
        store.subscribeListener(listener3, "test_namespace", "sample_streamer_3",
                Collections.emptyList(), ts1);

        // After a brief wait verify that the listener gets all the updates.
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        LinkedList<CorfuStreamEntries> updates1 = listener1.getUpdates();
        assertThat(updates1).hasSize(2);

        LinkedList<CorfuStreamEntries> updates2 = listener2.getUpdates();
        assertThat(updates2).hasSize(3);

        LinkedList<CorfuStreamEntries> updates3 = listener3.getUpdates();
        assertThat(updates3).isEmpty();

        index = 0;
        assertThat(updates1.get(0).getEntries()).hasSize(1);
        List<CorfuStreamEntry> entries = updates1.get(0).getEntries().values().stream().findFirst().get();
        assertThat(entries).hasSize(1);
        uuid = Uuid.newBuilder().setLsb(index).setMsb(index).build();
        msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
        assertThat(entries.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
        assertThat(entries.get(0).getKey()).isEqualTo(uuid);
        assertThat(entries.get(0).getPayload()).isEqualTo(msgA);
        assertThat(entries.get(0).getMetadata()).isEqualTo(uuid);

        assertThat(updates2.get(0).getEntries()).isEqualTo(updates1.get(0).getEntries());

        index++;
        assertThat(updates2.get(1).getEntries()).hasSize(1);
        entries = updates2.get(1).getEntries().values().stream().findFirst().get();
        assertThat(entries).hasSize(1);
        uuid = Uuid.newBuilder().setLsb(index).setMsb(index).build();
        msgB = SampleTableBMsg.newBuilder().setPayload(String.valueOf(index)).build();
        assertThat(entries.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
        assertThat(entries.get(0).getKey()).isEqualTo(uuid);
        assertThat(entries.get(0).getPayload()).isEqualTo(msgB);
        assertThat(entries.get(0).getMetadata()).isEqualTo(uuid);

        index++;
        assertThat(updates1.get(1).getEntries()).hasSize(1);
        entries = updates1.get(1).getEntries().values().stream().findFirst().get();
        assertThat(entries).hasSize(1);
        uuid = Uuid.newBuilder().setLsb(index).setMsb(index).build();
        msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
        assertThat(entries.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
        assertThat(entries.get(0).getKey()).isEqualTo(uuid);
        assertThat(entries.get(0).getPayload()).isEqualTo(msgA);
        assertThat(entries.get(0).getMetadata()).isEqualTo(uuid);

        assertThat(updates2.get(2).getEntries()).hasSize(2);
        assertThat(updates2.get(2).getEntries().keySet()).containsExactlyInAnyOrder(
                new TableSchema<>("tableA", Uuid.class, SampleTableAMsg.class, Uuid.class),
                new TableSchema<>("tableB", Uuid.class, SampleTableBMsg.class, Uuid.class)
        );

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test that streaming can work well when number of subscribers exceeds number of
     * threads in the thread pools and some subscribers are slow.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testMoreSubscribersAndSlowSubscribers() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = Timestamp.newBuilder().setEpoch(0L).setSequence(Address.NON_ADDRESS).build();

        // Create a table.
        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                "test_namespace", "tableA",
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );

        final int numThread = 2;
        final int numListener = numThread + 2;
        final int bufferSize = 10;
        final int numUpdates = bufferSize + 1;

        // Make some updates to the tables.
        for (int i = 0; i < numUpdates; i++) {
            SampleTableAMsg msg = SampleTableAMsg.newBuilder().setPayload(String.valueOf(i)).build();
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            TxnContext tx = store.txn("test_namespace");
            tx.putRecord(tableA, uuid, msg, uuid);
            tx.commit();
        }

        CountDownLatch latch = new CountDownLatch(1);
        StreamListenerImpl[] listeners = new StreamListenerImpl[numListener];

        // Some subscribers are blocked until the latch blocks them.
        // One thread in the pool will never blocked.
        for (int i = 0; i < numListener; i++) {
            if (i < numThread - 1) {
                listeners[i] = new BlockingStreamListener("listener" + i, latch);
            } else {
                listeners[i] = new StreamListenerImpl("listener" + i);
            }
            store.subscribeListener(listeners[i], "test_namespace", defaultTag,
                    Collections.singletonList("tableA"), ts1, bufferSize);
        }

        TimeUnit.MILLISECONDS.sleep(sleepTime * 2);

        for (int i = 0; i < numListener; i++) {
            if (i < numThread - 1) {
                assertThat(listeners[i].getUpdates().size()).isEqualTo(0);
            } else {
                assertThat(listeners[i].getUpdates().size()).isEqualTo(numUpdates);
            }
        }

        latch.countDown();
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        for (int i = 0; i < numListener; i++) {
            assertThat(listeners[i].getUpdates().size()).isEqualTo(numUpdates);
        }

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test the case where a table is empty at the time of subscription, and that it is able
     * to receive deltas once updates to the table happen.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSubscriberOnEmptyTables() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = Timestamp.newBuilder().setEpoch(0L).setSequence(Address.NON_ADDRESS).build();
        final String namespace = "test_namespace";
        final String tableName = "table_test";

        // Create 2 tables.
        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                namespace, tableName,
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );

        // Subscribe to streaming updates, while table has not been yet updated
        StreamListenerImpl listener1 = new StreamListenerImpl("stream_listener_1");
        store.subscribeListener(listener1, namespace, defaultTag,
                Collections.singletonList(tableName), ts1);

        // Wait for a while, so we are sure the subscriber poller has run
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        assertThatThrownBy(() -> store.subscribeListener(listener1, namespace, defaultTag,
                Collections.singletonList(tableName), ts1)).isExactlyInstanceOf(StreamingException.class);

        // Make some updates to the table
        final int numUpdates = 5;
        for (int index = 0; index < numUpdates; index++) {
            try (TxnContext tx = store.txn(namespace)) {
                Uuid uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleTableAMsg msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
                tx.putRecord(tableA, uuid, msgA, uuid);
                tx.commit();
            }
        }

        // Wait for a while, so we are sure the subscriber poller has run
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        // Verify updates
        LinkedList<CorfuStreamEntries> updates1 = listener1.getUpdates();
        assertThat(updates1).hasSize(numUpdates);

        for (int index = 0; index < numUpdates; index++) {
            assertThat(updates1.get(index).getEntries()).hasSize(1);
            List<CorfuStreamEntry> entries = updates1.get(index).getEntries().values().stream().findFirst().get();
            assertThat(entries).hasSize(1);
            assertThat(((Uuid)entries.get(0).getKey()).getMsb()).isEqualTo(index);
            assertThat(((SampleTableAMsg)entries.get(0).getPayload()).getPayload()).isEqualTo(String.valueOf(index));
            assertThat(((Uuid)entries.get(0).getMetadata()).getMsb()).isEqualTo(index);
        }

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test the case where a subscriber hits an error (for instance due to a poison pill) and
     * decides to re-subscribe from the last observed update (based on the provided timestamp).
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSubscriberTimestamp() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = Timestamp.newBuilder().setEpoch(0L).setSequence(Address.NON_ADDRESS).build();
        final String namespace = "test_namespace";
        final String tableNameA = "table_testA";
        final String tableNameB = "table_testB";

        // Create 2 tables.
        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                namespace, tableNameA,
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );

        Table<Uuid, SampleTableBMsg, Uuid> tableB = store.openTable(
                namespace, tableNameB,
                Uuid.class, SampleTableBMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableBMsg.class)
        );

        final int numUpdates = 10;

        // Subscribe to streaming for one 'tag' corresponding to TableA
        // This listener will artificially throw an Exception when numUpdates/2 are received, enforcing unsubscription
        // and testing the last provided timestamp truly tracks the last received update and can resume from that point
        // onwards (no data loss or need to sync a full snapshot)
        CountDownLatch latch = new CountDownLatch(1);
        StreamListenerImpl listener1 = new StreamListenerImpl("stream_listener_1", numUpdates / 2, latch);
        store.subscribeListener(listener1, namespace, defaultTag,
                Collections.singletonList(tableNameA), ts1);

        // Wait for a while, so we are sure the subscriber poller has run
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        // Confirm subscribe was successful
        assertThatThrownBy(() -> store.subscribeListener(listener1, namespace, defaultTag,
                Collections.singletonList(tableNameA), ts1)).isExactlyInstanceOf(StreamingException.class);

        // Make some updates to both tables
        for (int index = 0; index < numUpdates; index++) {
            // Update TableA
            try (TxnContext tx = store.txn(namespace)) {
                Uuid uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleTableAMsg msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
                tx.putRecord(tableA, uuid, msgA, uuid);
                tx.commit();
            }
            // Update TableB (just an interleaving table so the space of updates is not continuous)
            try (TxnContext tx = store.txn(namespace)) {
                Uuid uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleTableBMsg msgB = SampleTableBMsg.newBuilder().setPayload(String.valueOf(index)).build();
                tx.putRecord(tableB, uuid, msgB, uuid);
                tx.commit();
            }
        }

        // Block until the error has been triggered
        latch.await();

        // Verify Partial Updates were received (before error) and re-subscribe
        LinkedList<CorfuStreamEntries> updates1 = listener1.getUpdates();
        assertThat(updates1).hasSize(numUpdates/2);

        for (int index = 0; index < numUpdates/2; index++) {
            assertThat(updates1.get(index).getEntries()).hasSize(1);
            List<CorfuStreamEntry> entries = updates1.get(index).getEntries().values().stream().findFirst().get();
            assertThat(entries).hasSize(1);
            assertThat(((Uuid)entries.get(0).getKey()).getMsb()).isEqualTo(index);
            assertThat(((SampleTableAMsg)entries.get(0).getPayload()).getPayload()).isEqualTo(String.valueOf(index));
            assertThat(((Uuid)entries.get(0).getMetadata()).getMsb()).isEqualTo(index);
        }

        // Re-subscribe from the last synced point and verify we catch up on the correct differential
        store.subscribeListener(listener1, namespace, defaultTag,
                Collections.singletonList(tableNameA), listener1.getTimestamp());

        // Wait for a while, so we are sure the subscriber poller has run
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        LinkedList<CorfuStreamEntries> updates2 = listener1.getUpdates();
        assertThat(updates2).hasSize(numUpdates); // accumulated updates

        for (int index = numUpdates/2; index < numUpdates; index++) {
            assertThat(updates2.get(index).getEntries()).hasSize(1);
            List<CorfuStreamEntry> entries = updates2.get(index).getEntries().values().stream().findFirst().get();
            assertThat(entries).hasSize(1);
            assertThat(((Uuid)entries.get(0).getKey()).getMsb()).isEqualTo(index);
            assertThat(((SampleTableAMsg)entries.get(0).getPayload()).getPayload()).isEqualTo(String.valueOf(index));
            assertThat(((Uuid)entries.get(0).getMetadata()).getMsb()).isEqualTo(index);
        }

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Backward compatibility test.
     * <p>
     * Streaming Test with 2 different tables and a single streamer
     * <p>
     * The test creates two tables in the same namespace and makes updates to both in the same transaction.
     * A single streamer subscribes to updates from both.
     * <p>
     * The test verifies that the streamer receives updates from both tables and since the updates were made in
     * the same transaction, they are received in one CorfuStreamEntry.
     */
    @Test
    public void testStreamingMultiTableSingleListener() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = Timestamp.newBuilder().setEpoch(0L).setSequence(Address.NON_ADDRESS).build();

        // Create 2 tables in the same namespace.
        Table<Uuid, Uuid, Uuid> n1t1 = store.openTable(
                namespace, "t1", Uuid.class,
                Uuid.class, Uuid.class,
                TableOptions.fromProtoSchema(Uuid.class)
        );

        Table<Uuid, Uuid, Uuid> n1t2 = store.openTable(
                namespace, "t2", Uuid.class,
                Uuid.class, Uuid.class,
                TableOptions.fromProtoSchema(Uuid.class)
        );

        // Make an update to the tables in a transaction.
        final int t1_uuid = 5;
        final int t2_uuid = 10;
        Uuid t1Uuid = Uuid.newBuilder().setMsb(t1_uuid).setLsb(t1_uuid).build();
        Uuid t2Uuid = Uuid.newBuilder().setMsb(t2_uuid).setLsb(t2_uuid).build();
        try (TxnContext txnContext = store.txn(namespace)) {
            txnContext.putRecord(n1t1, t1Uuid, t1Uuid, t1Uuid);
            txnContext.putRecord(n1t2, t2Uuid, t2Uuid, t2Uuid);
            txnContext.commit();
        }

        // Subscribe to both tables.
        TableSchema<Uuid, Uuid, Uuid> schema1 = new TableSchema<>("t1", Uuid.class, Uuid.class, Uuid.class);
        TableSchema<Uuid, Uuid, Uuid> schema2 = new TableSchema<>("t2", Uuid.class, Uuid.class, Uuid.class);
        StreamListenerImpl listener = new StreamListenerImpl("n1_listener");
        store.subscribeListener(listener, namespace, "tag", Arrays.asList("t1", "t2"), ts1);

        // Verify that both updates come to the subscriber in the same StreamEntry.
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        LinkedList<CorfuStreamEntries> updates = listener.getUpdates();
        assertThat(updates.size()).isEqualTo(1);
        assertThat(updates.getFirst().getEntries().entrySet().size()).isEqualTo(2);

        // Check the entries and operations in each.
        assertThat(updates.getFirst().getEntries().get(schema1).get(0).getKey()).isEqualTo(t1Uuid);
        assertThat(updates.getFirst().getEntries().get(schema1).get(0).getPayload()).isEqualTo(t1Uuid);
        assertThat(updates.getFirst().getEntries().get(schema1).get(0).getMetadata()).isEqualTo(t1Uuid);
        assertThat(updates.getFirst().getEntries().get(schema1).get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);

        assertThat(updates.getFirst().getEntries().get(schema2).get(0).getKey()).isEqualTo(t2Uuid);
        assertThat(updates.getFirst().getEntries().get(schema2).get(0).getPayload()).isEqualTo(t2Uuid);
        assertThat(updates.getFirst().getEntries().get(schema2).get(0).getMetadata()).isEqualTo(t2Uuid);
        assertThat(updates.getFirst().getEntries().get(schema2).get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test the case where a snapshot of the database is captured and we stream from this snapshot onwards.
     * Verify deltas are not missed and that tx.commit() returns the correct timestamp.
     *
     * We will write data to 2 tables, but snapshot/stream on only one of them.
     *
     */
    @Test
    public void testFullSyncDeltaSyncPattern() throws Exception {
        // Run a corfu server & initialize CorfuStore
        CorfuRuntime runtime = initializeCorfu();

        final int numUpdates = 10;
        final int indexDefault= 0;
        // Write updates to table_A
        writeUpdatesToDefaultTable(numUpdates, 0);

        // Write updates to table_B
        writeUpdatesToRandomTable(numUpdates, 0, "table_B");

        // Perform Full Sync (snapshot)
        Timestamp snapshotTs;
        Uuid firstKey = Uuid.newBuilder().setMsb(indexDefault).setLsb(indexDefault).build();
        SampleTableAMsg firstValue = SampleTableAMsg.newBuilder().setPayload(String.valueOf(indexDefault)).build();

        // Full-Sync (one or more tables) read-only transaction
        try (TxnContext txn = store.txn(namespace)) {
            CorfuStoreEntry<Uuid, SampleTableAMsg, Uuid> entry = txn.getRecord(defaultTableName, firstKey);
            assertThat(entry.getPayload()).isEqualTo(firstValue);
            snapshotTs = txn.commit();
            assertThat(snapshotTs.getSequence()).isEqualTo(runtime.getSequencerView()
                    .query(FullyQualifiedTableName.streamId(namespace, defaultTableName).getId()));
        }

        // Subscribe from latest full sync
        StreamListenerImpl listener = new StreamListenerImpl("stream_listener");
        store.subscribeListener(listener, namespace, defaultTag, Arrays.asList(defaultTableName), snapshotTs);

        // Write more updates to table_B (not streaming)
        writeUpdatesToRandomTable(numUpdates, 0, "table_B");
        // Write more updates to table_A (streaming)
        writeUpdatesToDefaultTable(numUpdates/2, 0);

        // Give time for deltas to be processed
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        LinkedList<CorfuStreamEntries> updates = listener.getUpdates();
        assertThat(updates.size()).isEqualTo(numUpdates/2);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test the case where a tagged stream does not progress over time (no updates) and
     * several checkpoint cycles kick in, moving the trim mark beyond last synced point
     * (ensure a trimmed exception is not thrown as it is actually up to date)
     */
    @Test
    public void testStreamingConsecutiveCheckpoints() throws Exception {
        // Run a corfu server & initialize CorfuStore
        CorfuRuntime runtime = initializeCorfu();

        // Record the initial timestamp.
        Timestamp initTs = Timestamp.newBuilder().setEpoch(0L).setSequence(Address.NON_ADDRESS).build();

        final int totalUpdates = 20;
        final int numCheckpointTrimCycles = 5;
        final String randomTableName = "randomTable";

        // Write 'totalUpdates' to defaultTable (which we will be streaming) and another random table
        // (which writes to a different tagged stream), the idea is to
        writeUpdatesToDefaultTable(totalUpdates, 0);
        writeUpdatesToRandomTable(totalUpdates, 0, randomTableName);

        // Start Listener on defaultTable from initial timestamp
        StreamListenerImpl listener = new StreamListenerImpl("stream_listener");
        store.subscribeListener(listener, namespace, defaultTag, Collections.singletonList(defaultTableName), initTs);

        // Confirm totalUpdates are received
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        assertThat(listener.getUpdates().size()).isEqualTo(totalUpdates);

        long seqNumBeforeTrim = -1L; // Default value expected for empty streams on getHighestSequence()

        // Run X number of checkpoint/trim (on each run, trigger runtimeGC)
        for (int i = 0; i < numCheckpointTrimCycles; i++) {
            List<String> tables = Arrays.asList(defaultTableName, randomTableName);
            checkpointAndTrim(runtime, namespace, tables, false);
            runtime.getGarbageCollector().runRuntimeGC();
            long seqNumberAfterTrim = store.getHighestSequence(namespace, defaultTableName);
            assertThat(seqNumberAfterTrim).isEqualTo(seqNumBeforeTrim);
        }

        // Confirm first untrimmed address is higher than the last received update (confirm pointer is below global trim mark)
        assertThat(runtime.getAddressSpaceView().getTrimMark().getSequence()).isGreaterThan(listener.getTimestamp().getSequence());

        // Check listener was not unsubscribed due to a Trimmed Exception (as pointer was kept below the latest trim mark)
        assertThrows(StreamingException.class, () -> store.subscribeListener(listener, namespace, defaultTag,
                Collections.singletonList(defaultTableName)));

        // Add new updates to the stream being subscribed to, confirm data is received
        writeUpdatesToDefaultTable(totalUpdates, totalUpdates);
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        assertThat(listener.getUpdates().size()).isEqualTo(totalUpdates*2);
    }



    /**
     * Confirm client exceptions (during onNext processing) are not wrapped as corfu unrecoverable streaming exceptions.
     */
    @Test
    public void testClientExceptions() throws Exception {
        // Run a corfu server & initialize Corfu Store
        CorfuRuntime runtime = initializeCorfu();

        // Write a number of updates to a certain table
        final int numUpdates = 20;
        writeUpdatesToDefaultTable(numUpdates, 0);

        // Subscribe to defaultTag from start of Log
        Timestamp startTimestamp = Timestamp.newBuilder()
                .setEpoch(0L)
                .setSequence(-1L)
                .build();
        CountDownLatch onErrorLatch = new CountDownLatch(1);
        StreamListenerImpl listener = new StreamListenerImpl("listener_client_failure", numUpdates / 2, onErrorLatch);
        store.subscribeListener(listener, namespace, defaultTag, Arrays.asList(defaultTableName), startTimestamp);

        // Block until Client Exception has been received and onError has been triggered.
        onErrorLatch.await();

        // Verify the expected received deltas (before trigger of exception was received)
        LinkedList<CorfuStreamEntries> updates1 = listener.getUpdates();
        assertThat(updates1).hasSize(numUpdates/2);

        for (int index = 0; index < numUpdates/2; index++) {
            assertThat(updates1.get(index).getEntries()).hasSize(1);
            List<CorfuStreamEntry> entries = updates1.get(index).getEntries().values().stream().findFirst().get();
            assertThat(entries).hasSize(1);
            assertThat(((Uuid)entries.get(0).getKey()).getMsb()).isEqualTo(index);
            assertThat(((SampleTableAMsg)entries.get(0).getPayload()).getPayload()).isEqualTo(String.valueOf(index));
            assertThat(((Uuid)entries.get(0).getMetadata()).getMsb()).isEqualTo(index);
        }

        runtime.shutdown();
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test subscribe streaming API for which only streams tags are specified.
     *
     * (1) Test the case where tables with a common tag are opened in advance and by subscribing to the tag,
     * updates for all tables are received.
     *
     * (2) Test the case where tables have been registered on a different store, and we attempt to
     * subscribe without opening the tables (this should fail, as key, value & metadata schemas are required).
     *
     * (3) Test the case where partial tables belonging to a tag have been opened by the time of subscription (should still fail).
     */
    @Test
    public void testSubscribeAPIForTagsOnly() throws Exception {
        // Run a corfu server & initialize Corfu Store
        initializeCorfu();

        // Write a number of updates to a certain table
        final int numUpdates = 20;
        final int totalTables = 3;
        final String commonStreamTag = "sample_streamer_2";
        final String uniqueTag = "sample_streamer_1";
        final String table1Name = defaultTableName;
        final String table2Name  = "tableSampleB_2";
        final String table3Name  = "tableSampleB_3";

        writeUpdatesToDefaultTable(numUpdates, 0);
        writeUpdatesToRandomTable(numUpdates, 0, table2Name);
        writeUpdatesToRandomTable(numUpdates, 0, table3Name);

        // First CorfuStore used to open 3 tables (2 share the same tag)
        // Subscribe first store to commonStreamTag and verify updates for all 3 tables are received
        StreamListenerImpl listener = new StreamListenerImpl("stream_listener");
        store.subscribeListener(listener, namespace, commonStreamTag, Timestamp.newBuilder().setSequence(Address.NON_ADDRESS).setEpoch(0L).build());

        // Confirm totalUpdates are received
        TimeUnit.MILLISECONDS.sleep(sleepTime * 2);
        assertThat(listener.getUpdates().size()).isEqualTo(numUpdates*totalTables);

        // Second CorfuStore (initially no tables opened)
        CorfuRuntime newRuntime = createRuntime(singleNodeEndpoint);
        CorfuStore newStore = new CorfuStore(newRuntime);

        StreamListenerImpl newListener = new StreamListenerImpl("new_stream_listener");
        // Attempt to subscribe to 'commonStreamTag', it should fail as none of the tables labeled with this tag
        // have been opened
        assertThatThrownBy(() -> newStore.subscribeListener(newListener, namespace, commonStreamTag,
                    Timestamp.newBuilder().setSequence(Address.NON_ADDRESS).setEpoch(0L).build()))
                .isExactlyInstanceOf(IllegalArgumentException.class);

        // Open only 1 of the tables labeled with 'commonStreamTag'
        newStore.openTable(
                namespace, table2Name,
                Uuid.class, SampleTableBMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableBMsg.class)
        );

        // Attempt to subscribe to 'commonStreamTag', it should still fail as ALL of the tables labeled with this tag
        // have not yet been opened
        assertThatThrownBy(() -> newStore.subscribeListener(newListener, namespace, commonStreamTag,
                Timestamp.newBuilder().setSequence(Address.NON_ADDRESS).setEpoch(0L).build()))
                .isExactlyInstanceOf(IllegalArgumentException.class);

        // Open remaining 2 tables
        newStore.openTable(
                namespace, table3Name,
                Uuid.class, SampleTableBMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableBMsg.class)
        );

        newStore.openTable(
                namespace, table1Name,
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );

        // Attempt to register, now that 3 tables have been opened
        newStore.subscribeListener(newListener, namespace, commonStreamTag,
                Timestamp.newBuilder().setSequence(Address.NON_ADDRESS).setEpoch(0L).build());

        // Confirm totalUpdates are received
        TimeUnit.MILLISECONDS.sleep(sleepTime * 2);
        assertThat(newListener.getUpdates().size()).isEqualTo(numUpdates*totalTables);

        // Confirm a tag which is unique to 1 table, also works
        StreamListenerImpl listenerUniqueTag = new StreamListenerImpl("stream_listener_unique");
        newStore.subscribeListener(listenerUniqueTag, namespace, uniqueTag,
                Timestamp.newBuilder().setSequence(Address.NON_ADDRESS).setEpoch(0L).build());

        // Confirm totalUpdates are received
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        assertThat(listenerUniqueTag.getUpdates().size()).isEqualTo(numUpdates);
    }

    /**
     * Verify subscribe API for which no timestamp is specified.
     *
     * @throws Exception
     */
    @Test
    public void testSubscribeListenerNoTimestamp() throws Exception {
        final String defaultTag = "sample_streamer_1";

        // Run a corfu server & initialize Corfu Store
        initializeCorfu();

        // Open Default Table (if table is not opened, subscribing to a tag will not capture any tables)
        store.openTable(
                namespace, defaultTableName,
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );

        // Subscribe before any data is written to the tables, to verify the timestamp corresponds to the current state
        StreamListenerImpl listenerPreWrites = new StreamListenerImpl("stream_listener_pre");
        store.subscribeListener(listenerPreWrites, namespace, defaultTag);

        // Write a number of updates to 'default' table
        final int numUpdates = 50;
        writeUpdatesToDefaultTable(numUpdates, 0);

        // Confirm totalUpdates are received by preWrites listener
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        assertThat(listenerPreWrites.getUpdates().size()).isEqualTo(numUpdates);

        // Subscribe to tag belonging to 'default' table and confirm no updates are received (as it should subscribed in the current timestamp)
        StreamListenerImpl listenerPostWrites = new StreamListenerImpl("stream_listener_post");
        store.subscribeListener(listenerPostWrites, namespace, defaultTag);

        // Confirm NO updates are received by postWrite listener
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        assertThat(listenerPostWrites.getUpdates().size()).isZero();

        // Write extra updates, and confirm both listeners receive them
        writeUpdatesToDefaultTable(numUpdates, numUpdates);
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        assertThat(listenerPreWrites.getUpdates().size()).isEqualTo(numUpdates*2);
        assertThat(listenerPostWrites.getUpdates().size()).isEqualTo(numUpdates);
    }

    /**
     * Test that updates to the same key within a transaction boundary does not lead
     * to multiple stream notifications, i.e., duplication.
     *
     * @throws Exception
     */
    @Test
    public void testDeduplicationOfTxUpdates() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final String namespace = "test_namespace";
        final String tableName = "table";
        final int numUpdates = 20;
        final int delta = 30;

        // Create a table.
        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                namespace, tableName,
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );

        // Subscribe to streaming updates
        StreamListenerImpl listener = new StreamListenerImpl("stream_listener");
        store.subscribeListener(listener, namespace, defaultTag,
            Collections.singletonList(tableName));

        // Update same key multiple times within the same transaction
        try (TxnContext tx = store.txn(namespace)) {
            Uuid key1 = Uuid.newBuilder().setMsb(0).setLsb(0).build();

            // Update key 1 multiple times within the same transaction
            for (int i = 0; i < numUpdates; i++) {
                SampleTableAMsg msg = SampleTableAMsg.newBuilder().setPayload(String.valueOf(i)).build();
                tx.putRecord(tableA, key1, msg, key1);
            }

            // Add another (unique) key to test the case when another key is present in the transaction
            Uuid key2 = Uuid.newBuilder().setMsb(1).setLsb(1).build();
            tx.putRecord(tableA, key2, SampleTableAMsg.newBuilder().setPayload(String.valueOf(numUpdates + delta)).build(), key2);

            // Update key1 one last time so it interleaves with a unique key
            tx.putRecord(tableA, key1, SampleTableAMsg.newBuilder().setPayload(String.valueOf(numUpdates)).build(), key1);

            tx.commit();
       }

        // After a brief wait verify that the listener gets all the updates.
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        LinkedList<CorfuStreamEntries> updates = listener.getUpdates();
        assertThat(updates.size()).isEqualTo(1);
        // One accounts for key1 (the one duplicated) and the other accounts for the unique id (key2)
        updates.getFirst().getEntries().forEach((k, v) -> assertThat(v.size()).isEqualTo(2));

        // Confirm the update for the duplicated key is the last one in the transaction and the unique key is present as well
        Set<String> readValues = new HashSet<>();
        updates.getFirst().getEntries().forEach((k, v) -> v.forEach(entry -> readValues.add(((SampleTableAMsg)entry.getPayload()).getPayload())));
        assertThat(readValues).containsOnlyOnce(String.valueOf(numUpdates));
        assertThat(readValues).containsOnlyOnce(String.valueOf(numUpdates + delta));

        store.unsubscribeListener(listener);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Validate re-generation of DCNs/stream updates when using the touch API
     *
     * @throws Exception
     */
    @Test
    public void testRegenarationOfStreamingUpdates() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final String namespace = "test_namespace";
        final String tableName = "table_test";
        final int numUpdates = 5;

        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                namespace, tableName,
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );

        // CountDownLatch will track number of updates + touched entry +1
        CountDownLatch updateCountLatch = new CountDownLatch(numUpdates + 1);

        // Subscribe to streaming updates, while table has not been yet updated
        StreamListenerImpl listener = new StreamListenerImpl("stream_listener_test", updateCountLatch);
        store.subscribeListener(listener, namespace, defaultTag);

        // Make some updates to the table
        for (int index = 0; index < numUpdates; index++) {
            try (TxnContext tx = store.txn(namespace)) {
                Uuid uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleTableAMsg msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
                tx.putRecord(tableA, uuid, msgA, uuid);
                tx.commit();
            }
        }

        final int randomIndex = 2;
        // Touch one of the updated keys
        try (TxnContext tx = store.txn(namespace)) {
            Uuid key = Uuid.newBuilder().setMsb(randomIndex).setLsb(randomIndex).build();
            tx.touch(tableA, key);
            tx.commit();
        }
        // Wait for listener to receive updates
        updateCountLatch.await();

        // Verify updates
        LinkedList<CorfuStreamEntries> updates = listener.getUpdates();
        assertThat(updates).hasSize(numUpdates + 1);

        for (int index = 0; index < numUpdates; index++) {
            assertThat(updates.get(index).getEntries()).hasSize(1);
            List<CorfuStreamEntry> entries = updates.get(index).getEntries().values().stream().findFirst().get();
            assertThat(entries).hasSize(1);
            assertThat(((Uuid)entries.get(0).getKey()).getMsb()).isEqualTo(index);
            assertThat(((SampleTableAMsg)entries.get(0).getPayload()).getPayload()).isEqualTo(String.valueOf(index));
            assertThat(((Uuid)entries.get(0).getMetadata()).getMsb()).isEqualTo(index);
        }

        // Verify touched entry re-generates the streaming update
        assertThat(updates.get(numUpdates).getEntries()).hasSize(1);
        List<CorfuStreamEntry> entries = updates.get(numUpdates).getEntries().values().stream().findFirst().get();
        assertThat(entries).hasSize(1);
        assertThat(((Uuid)entries.get(0).getKey()).getMsb()).isEqualTo(randomIndex);
        assertThat(((SampleTableAMsg)entries.get(0).getPayload()).getPayload()).isEqualTo(String.valueOf(randomIndex));
        assertThat(((Uuid)entries.get(0).getMetadata()).getMsb()).isEqualTo(randomIndex);

        // Verify number of actual entries is not modified due to the 'touch' API
        assertThat(tableA.count()).isEqualTo(numUpdates);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    private void writeUpdatesToDefaultTable(int numUpdates, int offset) throws Exception {
        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                namespace, defaultTableName,
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );

        // Make some updates to tableA
        for (int index = offset; index < offset + numUpdates; index++) {
            // Update TableA
            try (TxnContext tx = store.txn(namespace)) {
                Uuid uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleTableAMsg msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
                tx.putRecord(tableA, uuid, msgA, uuid);
                tx.commit();
            }
        }
    }

    /**
     * Write updates to any given table with the following schema:
     *      key: Uuid
     *      value: SampleTableBMsg
     *      metadata: Uuid
     */
    private void writeUpdatesToRandomTable(int numUpdates, int offset, String tableName) throws Exception {
        Table<Uuid, SampleTableBMsg, Uuid> tableB = store.openTable(
                namespace, tableName,
                Uuid.class, SampleTableBMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableBMsg.class)
        );

        for (int index = offset; index < offset + numUpdates; index++) {
            try (TxnContext tx = store.txn(namespace)) {
                Uuid uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleTableBMsg msgB = SampleTableBMsg.newBuilder().setPayload(String.valueOf(index)).build();
                tx.putRecord(tableB, uuid, msgB, uuid);
                tx.commit();
            }
        }
    }

    private CorfuRuntime initializeCorfu() throws Exception {
        corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        store = new CorfuStore(runtime);

        return runtime;
    }
}
