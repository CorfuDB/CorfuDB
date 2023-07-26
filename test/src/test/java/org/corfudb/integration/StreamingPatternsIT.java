package org.corfudb.integration;

import com.google.common.collect.Iterables;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.StreamListenerResumeOrDefault;
import org.corfudb.runtime.collections.StreamListenerResumeOrFullSync;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleSchema.EventInfo;
import org.corfudb.test.SampleSchema.ManagedResources;
import org.corfudb.test.SampleSchema.SampleTableAMsg;
import org.corfudb.test.SampleSchema.SampleTableBMsg;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Simple test that inserts data into CorfuStore and tests Streaming.
 */
@SuppressWarnings("checkstyle:magicnumber")
@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class StreamingPatternsIT extends AbstractIT {

    private static String corfuSingleNodeHost;
    private static int corfuStringNodePort;
    private static String singleNodeEndpoint;
    private Process corfuServer;
    private CorfuStore store;

    private final String namespace = "UT-namespace";
    private final String defaultTableName = "table_default";
    private final String defaultTag = "sample_streamer_1";

    private final int sleepTime = 300;

    private Table<Uuid, SampleTableAMsg, Uuid> tableDefault;

    /**
     * Load properties for a single node corfu server before each test
     */
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    /**
     * Test stream listener implementation with default re-subscription policy, i.e.,
     * resume subscription if possible or subscribe to latest point in the log when not possible.
     */
    private class StreamListenerResumeOrDefaultImpl extends StreamListenerResumeOrDefault {

        private int updatesToError = 0;
        private final CountDownLatch errorNotifierLatch;

        @Getter
        private final List<CorfuStreamEntries> updates = Collections.synchronizedList(new LinkedList<>());


        public StreamListenerResumeOrDefaultImpl(CorfuStore store, String namespace, String streamTag,
                                                 int updatesToError, CountDownLatch errorNotifierLatch) {
            super(store, namespace, streamTag, null);
            this.updatesToError = updatesToError;
            this.errorNotifierLatch = errorNotifierLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            if (updates.size() == updatesToError) {
                // Decrement updatesToError by 1 so further updates are processed, after the error
                updatesToError -= 1;
                throw new IllegalStateException("Artificial exception to trigger onError");
            }
            log.info("on next adding {}", results.getTimestamp());
            updates.add(results);
        }

        @Override
        public void onError(Throwable throwable) {
            // Notify subscriber of error
            errorNotifierLatch.countDown();

            try {
                // Wait for signal from subscriber to resume onError
                errorNotifierLatch.await();
            } catch (InterruptedException ie) {
                log.error("Caught Interrupted Exception");
            }

            super.onError(throwable);
        }
    }

    /**
     * Test stream listener implementation with full sync re-subscription policy, i.e.,
     * resume subscription if possible or full sync and re-subscribe when not possible.
     */
    private class StreamListenerResumeOrFullSyncImpl extends StreamListenerResumeOrFullSync {

        private int updatesToError = 0;
        private final CountDownLatch errorNotifierLatch;
        private final CountDownLatch fullSyncLatch;
        private boolean failOnFullSync = false;

        @Getter
        private final Collection<CorfuStreamEntry> updates = Collections.synchronizedCollection(new ArrayList<>());

        @Getter
        private final Collection<CorfuStoreEntry> fullSyncBuffer = Collections.synchronizedCollection(new ArrayList<>());

        public StreamListenerResumeOrFullSyncImpl(CorfuStore store, String namespace, String streamTag,
                                                  int updatesToError, CountDownLatch errorNotifierLatch, CountDownLatch fullSyncLatch) {
            super(store, namespace, streamTag, null);
            this.updatesToError = updatesToError;
            this.errorNotifierLatch = errorNotifierLatch;
            this.fullSyncLatch = fullSyncLatch;
        }

        public StreamListenerResumeOrFullSyncImpl(CorfuStore store, String namespace, String streamTag,
                                                  int updatesToError, CountDownLatch errorNotifierLatch,
                                                  CountDownLatch fullSyncLatch,
                                                  boolean failOnFullSync) {
            super(store, namespace, streamTag, null);
            this.updatesToError = updatesToError;
            this.errorNotifierLatch = errorNotifierLatch;
            this.failOnFullSync = failOnFullSync;
            this.fullSyncLatch = fullSyncLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            if (updates.size() == updatesToError) {
                // Decrement updatesToError by 1 so further updates are processed, after the error
                updatesToError -= 1;
                throw new IllegalStateException("Artificial exception to trigger onError");
            }

            results.getEntries().values().forEach(updates::addAll);
        }

        @Override
        public void onError(Throwable throwable) {
            // Notify subscriber of error
            errorNotifierLatch.countDown();

            try {
                // Wait for signal from subscriber to resume onError
                errorNotifierLatch.await();
            } catch (InterruptedException ie) {
                log.error("Caught Interrupted Exception");
            }

            super.onError(throwable);
        }

        @Override
        protected Timestamp performFullSync() {

            Timestamp fullSyncTs;

            if (failOnFullSync) {
                throw new IllegalArgumentException("performFullSync logic exception!!");
            }

            // Reset buffer
            updates.clear();

            // Full-Sync table
            try (TxnContext txn = store.txn(namespace)) {
                Table<Uuid, SampleTableAMsg, Uuid> table = txn.getTable(defaultTableName);
                table.entryStream().forEach(fullSyncBuffer::add);
                fullSyncTs = txn.commit();
            }

            // Flag full sync completed
            fullSyncLatch.countDown();

            return fullSyncTs;
        }
    }

    /**
     * This test verifies the 'resume' functionality for the resumeOrDefault re-subscription policy pattern.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testResumePortionOfResumeOrDefaultPolicy() throws Exception {
        // Run a corfu server & initialize CorfuStore
        initializeCorfu();

        // Record initial timestamp, to subscribe from the start of the log
        Timestamp startLog = Timestamp.newBuilder().setEpoch(0L).setSequence(-1L).build();

        // Write initial updates to default table
        final int numUpdates = 10;
        final int processedBeforeError = numUpdates - 1;
        openDefaultTable();
        updateDefaultTable(numUpdates, 0);

        // Create default stream listener
        CountDownLatch errorLatch = new CountDownLatch(2);
        StreamListenerResumeOrDefaultImpl defaultStreamListener = new StreamListenerResumeOrDefaultImpl(store,
                namespace, defaultTag, processedBeforeError, errorLatch);

        // Subscribe listener, and wait for the (enforced) error to be thrown
        // (which is thrown once all updates have been received)
        store.subscribeListener(defaultStreamListener, namespace, defaultTag, startLog);

        do {
            // Wait until latch has decreased by '1' (error is triggered)
            log.trace("Wait onError to be triggered");
        } while (errorLatch.getCount() != 1);

        assertThat(defaultStreamListener.getUpdates()).hasSize(processedBeforeError);

        // Generate new updates (while still not re-subscribed after onError)
        // - to confirm it resumes from the last point
        // Generate some random (interleaved entries to another table)
        // - to confirm it is not polluting the listeners.
        updateRandomTable(numUpdates);
        updateDefaultTable(numUpdates, numUpdates);

        // Wait for a while to confirm listener has stopped receiving updates
        TimeUnit.MILLISECONDS.sleep(sleepTime * 4);
        assertThat(defaultStreamListener.getUpdates()).hasSize(processedBeforeError);

        // Unblock listener so it resumes subscription
        errorLatch.countDown();

        // Wait for a while so listener is re-subscribed and confirm it has synced from the last point (not lost the
        // numUpdates added while unsubscribed)
        TimeUnit.MILLISECONDS.sleep(sleepTime * 4);
        assertThat(defaultStreamListener.getUpdates()).hasSize(numUpdates*2);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test verifies the 'default' re-subscription functionality for the resumeOrDefault when
     * resume is not possible.
     */
    @Test
    public void testDefaultPortionOfResumeOrDefaultPolicy() throws Exception {
        // Run a corfu server & initialize CorfuStore
        CorfuRuntime runtime = initializeCorfu();

        // Record initial timestamp, to subscribe from the start of the log
        Timestamp startLog = Timestamp.newBuilder().setEpoch(0L).setSequence(-1L).build();

        // Write initial updates to default table
        final int numUpdates = 10;
        final int deltaToError = 1;
        openDefaultTable();
        updateDefaultTable(numUpdates, 0);

        // Create default stream listener
        CountDownLatch errorLatch = new CountDownLatch(2);
        StreamListenerResumeOrDefaultImpl defaultStreamListener = new StreamListenerResumeOrDefaultImpl(store,
                namespace, defaultTag, numUpdates - deltaToError, errorLatch);

        // Subscribe listener, and wait for the (enforced) error to be thrown (which is thrown once
        // all updates have been received)
        store.subscribeListener(defaultStreamListener, namespace, defaultTag, startLog);

        do {
            // Wait until latch has decreased by '1' (error is triggered)
            TimeUnit.MILLISECONDS.sleep(100);
        } while (errorLatch.getCount() != 1);

        assertThat(defaultStreamListener.getUpdates()).hasSize(numUpdates - deltaToError);

        // Trim the log on the last processed entry + delta, to confirm it is not able to resume and it runs the
        // default policy (skipping several entries)
        MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw = new MultiCheckpointWriter<>();
        List<String> tablesToCheckpoint = Arrays.asList(defaultTableName);
        tablesToCheckpoint.forEach(tableName -> {
            PersistentCorfuTable<Uuid, CorfuRecord<EventInfo, ManagedResources>> corfuTable =
                    createCorfuTable(runtime, TableRegistry.getFullyQualifiedTableName(namespace, tableName));

            mcw.addMap(corfuTable);
        });

        // Add new updates to current table (while it has not re-subscribed)
        updateDefaultTable(numUpdates, numUpdates);

        // Add Registry Table
        mcw.addMap(runtime.getTableRegistry().getRegistryTable());
        mcw.addMap(runtime.getTableRegistry().getProtobufDescriptorTable());
        mcw.appendCheckpoints(runtime, "StreamingPatternsIT");

        Timestamp lastProcessedTs = Iterables.getLast(defaultStreamListener.getUpdates()).getTimestamp();
        Token trimPoint = new Token(lastProcessedTs.getEpoch(),
                lastProcessedTs.getSequence() + deltaToError + (numUpdates/2));
        runtime.getAddressSpaceView().prefixTrim(trimPoint);
        runtime.getAddressSpaceView().gc();
        runtime.getObjectsView().getObjectCache().clear();

        // Unblock listener so it 'attempts' to resume subscription (should fail due to TrimmedException)
        errorLatch.countDown();

        // Wait while default re-subscription policy kicks in
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        // Confirm no further updates have been processed, as the default policy should
        // subscribe from the end of the log
        assertThat(defaultStreamListener.getUpdates()).hasSize(numUpdates - deltaToError);

        // Add new updates to current table, confirm they are processed by listener
        // (as they're beyond the point of re-subscription)
        updateDefaultTable(numUpdates, numUpdates);
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        assertThat(defaultStreamListener.getUpdates()).hasSize(numUpdates*2 - deltaToError);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test verifies the 'resume' functionality for the resumeOrFullSync re-subscription policy pattern.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testResumePortionOfResumeOrFullSyncPolicy() throws Exception {
        // Run a corfu server & initialize CorfuStore
        initializeCorfu();

        // Record initial timestamp, to subscribe from the start of the log
        Timestamp startLog = Timestamp.newBuilder().setEpoch(0L).setSequence(-1L).build();

        // Write initial updates to default table
        final int numUpdates = 10;
        final int processedBeforeError = numUpdates - 1;
        openDefaultTable();
        updateDefaultTable(numUpdates, 0);

        // Create default stream listener
        CountDownLatch errorLatch = new CountDownLatch(2);
        StreamListenerResumeOrFullSyncImpl defaultStreamListener = new StreamListenerResumeOrFullSyncImpl(store,
                namespace, defaultTag, processedBeforeError, errorLatch, null);

        // Subscribe listener, and wait for the (enforced) error to be thrown
        // (which is thrown once all updates have been received)
        store.subscribeListener(defaultStreamListener, namespace, defaultTag, startLog);

        do {
            // Wait until latch has decreased by '1' (error is triggered)
            log.trace("Wait onError to be triggered");
        } while (errorLatch.getCount() != 1);

        assertThat(defaultStreamListener.getUpdates()).hasSize(processedBeforeError);

        // Generate new updates (while still not re-subscribed after onError)
        // - to confirm it resumes from the last point
        // Generate some random (interleaved entries to another table)
        // - to confirm it is not polluting the listeners.
        updateRandomTable(numUpdates);
        updateDefaultTable(numUpdates, numUpdates);

        // Wait for a while to confirm listener has stopped receiving updates
        TimeUnit.MILLISECONDS.sleep(sleepTime * 5);
        assertThat(defaultStreamListener.getUpdates()).hasSize(processedBeforeError);

        // Unblock listener so it resumes subscription
        errorLatch.countDown();

        // Wait for a while so listener is re-subscribed and confirm it has synced from the last point (not lost the
        // numUpdates added while unsubscribed)
        TimeUnit.MILLISECONDS.sleep(sleepTime * 4);
        assertThat(defaultStreamListener.getUpdates()).hasSize(numUpdates*2);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test verifies the 'fullSync' re-subscription functionality for the resumeOrFullSync when
     * resume is not possible.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testFullSyncPortionOfResumeOrFullSyncPolicy() throws Exception {
        // Run a corfu server & initialize CorfuStore
        CorfuRuntime runtime = initializeCorfu();

        // Record initial timestamp, to subscribe from the start of the log
        Timestamp startLog = Timestamp.newBuilder().setEpoch(0L).setSequence(-1L).build();

        // Write initial updates to default table
        final int numUpdates = 10;
        final int deltaToError = 1;
        openDefaultTable();
        updateDefaultTable(numUpdates, 0);

        // Create default stream listener
        CountDownLatch errorLatch = new CountDownLatch(2);
        CountDownLatch fullSyncLatch = new CountDownLatch(1);
        StreamListenerResumeOrFullSyncImpl defaultStreamListener = new StreamListenerResumeOrFullSyncImpl(store,
                namespace, defaultTag, numUpdates - deltaToError, errorLatch, fullSyncLatch);

        // Subscribe listener, and wait for the (enforced) error to be thrown
        // (which is thrown once all updates have been received)
        store.subscribeListener(defaultStreamListener, namespace, defaultTag, startLog);

        do {
            // Wait until latch has decreased by '1' (error is triggered)
        } while (errorLatch.getCount() != 1);

        assertThat(defaultStreamListener.getUpdates()).hasSize(numUpdates - deltaToError);

        // Add new updates to current table (while it has not re-subscribed) that will be subsumed in the checkpoint
        updateDefaultTable(numUpdates, numUpdates);

        // Trim the log on the last processed entry + delta, to confirm it is not able to resume and it runs the
        // default policy (skipping several entries)
        MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw = new MultiCheckpointWriter<>();
        List<String> tablesToCheckpoint = Arrays.asList(defaultTableName);
        tablesToCheckpoint.forEach(tableName -> {
            PersistentCorfuTable<Uuid, CorfuRecord<EventInfo, ManagedResources>> corfuTable =
                    createCorfuTable(runtime, TableRegistry.getFullyQualifiedTableName(namespace, tableName));

            mcw.addMap(corfuTable);
        });

        // Add Registry Table
        mcw.addMap(runtime.getTableRegistry().getRegistryTable());
        mcw.addMap(runtime.getTableRegistry().getProtobufDescriptorTable());
        Token trimToken = mcw.appendCheckpoints(runtime, "StreamingPatternsIT");
        runtime.getAddressSpaceView().prefixTrim(trimToken);
        runtime.getAddressSpaceView().gc();
        runtime.getObjectsView().getObjectCache().clear();

        // Unblock listener so it 'attempts' to resume subscription (should fail due to TrimmedException)
        errorLatch.countDown();

        // Wait while default re-subscription policy kicks in
        TimeUnit.MILLISECONDS.sleep(sleepTime * 5);

        // Confirm no updates have been processed
        // (as it was reset by full sync and all updates are part of the checkpoint)
        // Block until full sync is completed
        fullSyncLatch.await();
        assertThat(defaultStreamListener.getUpdates()).isEmpty();
        assertThat(defaultStreamListener.getFullSyncBuffer().size()).isEqualTo(numUpdates*2);

        // Add new updates to current table, confirm they are processed by listener
        // (as they're beyond the point of re-subscription)
        updateDefaultTable(numUpdates, numUpdates);

        TimeUnit.MILLISECONDS.sleep(sleepTime * 3);
        assertThat(defaultStreamListener.getUpdates()).hasSize(numUpdates);
        assertThat(defaultStreamListener.getFullSyncBuffer().size()).isEqualTo(numUpdates*2);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test verifies errors on the 'fullSync' method for the resumeOrFullSync when
     * resume is not possible.
     */
    @Test
    public void testFullSyncFailureOfResumeOrFullSyncPolicy() throws Exception {
        // Run a corfu server & initialize CorfuStore
        CorfuRuntime runtime = initializeCorfu();

        // Record initial timestamp, to subscribe from the start of the log
        Timestamp startLog = Timestamp.newBuilder().setEpoch(0L).setSequence(-1L).build();

        // Write initial updates to default table
        final int numUpdates = 10;
        final int deltaToError = 1;
        openDefaultTable();
        updateDefaultTable(numUpdates, 0);

        // Create default stream listener
        CountDownLatch errorLatch = new CountDownLatch(2);
        StreamListenerResumeOrFullSyncImpl defaultStreamListener = new StreamListenerResumeOrFullSyncImpl(store,
                namespace, defaultTag, numUpdates - deltaToError, errorLatch, null, true);

        // Subscribe listener, and wait for the (enforced) error to be thrown (which is thrown once all updates have been received)
        store.subscribeListener(defaultStreamListener, namespace, defaultTag, startLog);

        do {
            // Wait until latch has decreased by '1' (error is triggered)
        } while (errorLatch.getCount() != 1);

        assertThat(defaultStreamListener.getUpdates()).hasSize(numUpdates - deltaToError);

        // Add new updates to current table (while it has not re-subscribed) that will be subsumed in the checkpoint
        updateDefaultTable(numUpdates, numUpdates);

        // Trim the log on the last processed entry + delta, to confirm it is not able to resume and it runs the
        // default policy (skipping several entries)
        MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw = new MultiCheckpointWriter<>();
        List<String> tablesToCheckpoint = Arrays.asList(defaultTableName);
        tablesToCheckpoint.forEach(tableName -> {
            PersistentCorfuTable<Uuid, CorfuRecord<EventInfo, ManagedResources>> corfuTable =
                    createCorfuTable(runtime, TableRegistry.getFullyQualifiedTableName(namespace, tableName));

            mcw.addMap(corfuTable);
        });

        // Add Registry Table
        mcw.addMap(runtime.getTableRegistry().getRegistryTable());
        mcw.addMap(runtime.getTableRegistry().getProtobufDescriptorTable());
        Token trimToken = mcw.appendCheckpoints(runtime, "StreamingPatternsIT");
        runtime.getAddressSpaceView().prefixTrim(trimToken);
        runtime.getAddressSpaceView().gc();
        runtime.getObjectsView().getObjectCache().clear();

        // Unblock listener so it 'attempts' to resume subscription (should fail due to TrimmedException)
        errorLatch.countDown();

        // Wait while default re-subscription policy kicks in
        TimeUnit.MILLISECONDS.sleep(sleepTime);

        // Confirm reset did not occur as a failure on full Sync happened and listener is not able to re-subscribe
        assertThat(defaultStreamListener.getUpdates()).isNotEmpty();
        assertThat(defaultStreamListener.getFullSyncBuffer().size()).isZero();
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    private void openDefaultTable() throws Exception {
        tableDefault = store.openTable(
                namespace, defaultTableName,
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );
    }

    private void updateDefaultTable(int numUpdates, int offset) {
        for (int index = offset; index < offset + numUpdates; index++) {
            try (TxnContext tx = store.txn(namespace)) {
                Uuid uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleTableAMsg msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
                tx.putRecord(tableDefault, uuid, msgA, uuid);
                tx.commit();
            }
        }
    }

    private void updateRandomTable(int numUpdates) throws Exception {
        Table<Uuid, SampleTableBMsg, Uuid> randomTable = store.openTable(
                namespace, "tableRandom",
                Uuid.class, SampleTableBMsg.class, Uuid.class,
                TableOptions.fromProtoSchema(SampleTableBMsg.class)
        );

        for (int index = 0; index < numUpdates; index++) {
            try (TxnContext tx = store.txn(namespace)) {
                Uuid uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleTableBMsg msgB = SampleTableBMsg.newBuilder().setPayload(String.valueOf(index)).build();
                tx.putRecord(randomTable, uuid, msgB, uuid);
                tx.commit();
            }
        }
    }

    /**
     * A helper method to initialize a Corfu Server, Corfu Runtime and Corfu Store instance.
     *
     * @return corfu runtime
     * @throws Exception error
     */
    private CorfuRuntime initializeCorfu() throws Exception {
        corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        store = new CorfuStore(runtime);

        return runtime;
    }

    /**
     * A helper method that takes host and port specification, start a single server and
     * returns a process.
     */
    private Process runSinglePersistentServer(String host, int port) throws IOException {
        return new CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();
    }
}
