package org.corfudb.integration;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.CorfuTestParameters;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValue;
import org.corfudb.infrastructure.logreplication.proto.Sample.Metadata;
import org.corfudb.infrastructure.logreplication.proto.Sample.StringKey;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationStateType;
import org.corfudb.infrastructure.logreplication.replication.fsm.ObservableAckMsg;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationUpgradeManager;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.Utils;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.integration.LogReplicationAbstractIT.checkpointAndTrimCorfuStore;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;

/**
 * Test the core components of log replication, namely, Snapshot Sync and Log Entry Sync,
 * i.e., the ability to transfer a full view (snapshot) or incremental view of the datastore
 * from a source to a destination. In these tests we disregard communication channels between
 * clusters (sites) or CorfuLogReplicationServer.
 *
 * We emulate the channel by implementing a test data plane which directly forwards the data
 * to the SinkManager. Overall, these tests bring up two CorfuServers (datastore components),
 * one performing as the source and the other as the sink. We write different patterns of data
 * on the source (transactional and non transactional, as well as polluted and non-polluted transactions, i.e.,
 * transactions containing federated and non-federated streams) and verify that complete data
 * reaches the destination after initiating log replication.
 */
@Slf4j
public class LogReplicationIT extends AbstractIT implements Observer {

    public static final String nettyConfig = "./test/src/test/resources/transport/nettyConfig.properties";

    private static final String SOURCE_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    private static final int WRITER_PORT = DEFAULT_PORT + 1;
    private static final String DESTINATION_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;
    private static final String REMOTE_CLUSTER_ID = UUID.randomUUID().toString();
    private static final int CORFU_PORT = 9000;
    private static final String TABLE_PREFIX = "test";

    private static final String TEST_NAMESPACE = "LR-Test";

    static private final int NUM_KEYS = 10;

    static private final int NUM_KEYS_LARGE = 1000;
    static private final int NUM_KEYS_VERY_LARGE = 20000;

    static private final int NUM_STREAMS = 1;
    static private final int TOTAL_STREAM_COUNT = 3;
    static private final int WRITE_CYCLES = 4;

    static private final int STATE_CHANGE_CHECKS = 20;
    static private final int WAIT_STATE_CHANGE = 300;

    // If testConfig set deleteOp enabled, will have one delete operation for four put operations.
    static private final int DELETE_PACE = 4;

    // Number of messages per batch
    static private final int BATCH_SIZE = 4;

    static private final int SMALL_MSG_SIZE = 12000;

    static private TestConfig testConfig = new TestConfig();

    // Connect with sourceServer to generate data
    private CorfuRuntime srcDataRuntime = null;

    // Connect with destinationServer to verify data
    private CorfuRuntime dstDataRuntime = null;

    private CorfuStore srcCorfuStore = null;

    private CorfuStore dstCorfuStore = null;

    private SourceForwardingDataSender sourceDataSender;

    // List of all opened maps backed by Corfu on Source and Destination
    private Map<String, Table<StringKey, IntValue, Metadata>> srcCorfuTables = new HashMap<>();
    private Map<String, Table<StringKey, IntValue, Metadata>> dstCorfuTables = new HashMap<>();

    // The in-memory data for corfu tables for verification.
    private Map<String, Map<String, Integer>> srcDataForVerification = new HashMap<>();
    private Map<String, Map<String, Integer>> dstDataForVerification = new HashMap<>();

    LogReplicationSourceManager logReplicationSourceManager;

    private CorfuRuntime srcTestRuntime;

    private CorfuRuntime dstTestRuntime;

    /* ********* Test Observables ********** */

    // An observable value on the number of received ACKs (source side)
    private ObservableAckMsg ackMessages;

    // An observable value on the metadata response (received by the source)
    private ObservableValue<LogReplicationMetadataResponseMsg> metadataResponseObservable;

    // An observable value on the number of errors received on Log Entry Sync (source side)
    private ObservableValue errorsLogEntrySync;

    // An observable value oln the number of received messages in the LogReplicationSinkManager (Destination Site)
    private ObservableValue sinkReceivedMessages;

    /* ******** Expected Values on Observables ******** */

    // Set per test according to the expected number of ACKs that will unblock the code waiting for the value change
    private long expectedAckMessages = 0;

    // Set per test according to the expected ACK's timestamp.
    private volatile AtomicLong expectedAckTimestamp;

    // Set per test according to the expected number of errors in a test
    private int expectedErrors = 1;

    // Set per test according to the expected number
    private int expectedSinkReceivedMessages = 0;

    private LogReplicationEntryType expectedAckMsgType = LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE;

    /* ********* Semaphore to block until expected values are reached ********** */

    // A semaphore that allows to block until the observed value reaches the expected value
    private final Semaphore blockUntilExpectedValueReached = new Semaphore(1, true);

    // A semaphore that allows to block until the observed value reaches the expected value
    private final Semaphore blockUntilExpectedAckType = new Semaphore(1, true);

    // A semaphore that allows to block until observed metadata response indicates completeness
    private final Semaphore blockUntilExpectedMetadataResponse = new Semaphore(1, true);

    private final Semaphore blockUntilExpectedAckTs = new Semaphore(1, true);

    private LogReplicationMetadataManager logReplicationMetadataManager;

    private final String t0Name = TABLE_PREFIX + 0;
    private final String t1Name = TABLE_PREFIX + 1;
    private final String t2Name = TABLE_PREFIX + 2;

    private final String t0NameUFO = TEST_NAMESPACE + "$" + t0Name;
    private final String t1NameUFO = TEST_NAMESPACE + "$" + t1Name;
    private final String t2NameUFO = TEST_NAMESPACE + "$" + t2Name;

    private final CountDownLatch blockUntilFSMTransition = new CountDownLatch(1);

    public LogReplicationIT() {
        PARAMETERS = new CorfuTestParameters(Duration.ofMinutes(5));
    }

    /**
     * Setup Test Environment
     *
     * - Two independent Corfu Servers (source and destination)
     * - CorfuRuntime's to each Corfu Server
     *
     * @throws IOException
     */
    private void setupEnv() throws IOException {
        // Source Corfu Server (data will be written to this server)
        new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        // Destination Corfu Server (data will be replicated into this server)
        new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        srcDataRuntime = CorfuRuntime.fromParameters(params);
        srcDataRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        srcDataRuntime.connect();

        srcTestRuntime = CorfuRuntime.fromParameters(params);
        srcTestRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        srcTestRuntime.connect();

        dstDataRuntime = CorfuRuntime.fromParameters(params);
        dstDataRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        dstDataRuntime.connect();

        dstTestRuntime = CorfuRuntime.fromParameters(params);
        dstTestRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        dstTestRuntime.connect();

        srcCorfuStore = new CorfuStore(srcDataRuntime);
        dstCorfuStore = new CorfuStore(dstDataRuntime);

        logReplicationMetadataManager = new LogReplicationMetadataManager(dstTestRuntime, 0, REMOTE_CLUSTER_ID);
        expectedAckTimestamp = new AtomicLong(Long.MAX_VALUE);
        testConfig.clear().setRemoteClusterId(REMOTE_CLUSTER_ID);
    }

    private void cleanEnv() {
        log.info("*** Clean environment");

        if (sourceDataSender != null) {
            sourceDataSender.shutdown();
        }

        if (srcDataRuntime != null) {
            srcDataRuntime.shutdown();
            srcTestRuntime.shutdown();
            dstDataRuntime.shutdown();
            dstTestRuntime.shutdown();
        }
    }

    /**
     * Open numStreams with fixed names 'TABLE_PREFIX' index and add them to 'tables'. All the opened streams will be
     * by default for replication (i.e. the is_federated flag is set to true). Some tests need to manually config certain
     * streams if they do not expect to be replicated.
     *
     * @param tables map to populate with opened streams
     * @param corfuStore corfu store
     * @param numStreams number of streams to open
     */
    private void openStreams(Map<String, Table<StringKey, IntValue, Metadata>> tables, CorfuStore corfuStore,
                             int numStreams) throws Exception {
        for (int i = 0; i < numStreams; i++) {
            String name = TABLE_PREFIX + i;
            Table<StringKey, IntValue, Metadata> table = corfuStore.openTable(
                    TEST_NAMESPACE,
                    name,
                    StringKey.class,
                    IntValue.class,
                    Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class)
            );
            tables.put(table.getFullyQualifiedTableName(), table);
        }
    }

    /**
     * Generate Transactional data on 'tables' and push the data for
     * further verification into an in-memory copy 'tablesForVerification'.
     */
    private void generateTXData(Map<String, Table<StringKey, IntValue, Metadata>> tables,
                      Map<String, Map<String, Integer>> hashMap,
                      int numKeys, CorfuStore corfuStore, int startValue) {
        for (String streamName : tables.keySet()) {
            generateTransactionsCrossTables(tables, Collections.singleton(streamName), hashMap, numKeys, corfuStore, startValue);
        }
    }

    /**
     * Generate Transactional data across several 'tablesCrossTxs' and push the data for
     * further verification into an in-memory copy 'tablesForVerification'.
     */
    private void generateTransactionsCrossTables(Map<String, Table<StringKey, IntValue, Metadata>> tables,
                                                 Set<String> tablesCrossTxs,
                                                 Map<String, Map<String, Integer>> tablesForVerification,
                                                 int numKeys, CorfuStore corfuStore, int startValue) {
        long tail = 0;
        for (int i = 0; i < numKeys; i++) {
            try (TxnContext txn = corfuStore.txn(TEST_NAMESPACE)) {
                for (String name : tablesCrossTxs) {
                    int key = i + startValue;
                    txn.putRecord(tables.get(name), StringKey.newBuilder().setKey(String.valueOf(key)).build(),
                        IntValue.newBuilder().setValue(key).build(), null);
                    tablesForVerification.putIfAbsent(name, new HashMap<>());
                    tablesForVerification.get(name).put(String.valueOf(key), key);
                }
                tail = txn.commit().getSequence();
            }
            expectedAckTimestamp.set(Math.max(tail, expectedAckTimestamp.get()));
        }
    }


    private void verifyTables(Map<String, Table<StringKey, IntValue, Metadata>> tables0,
                              Map<String, Table<StringKey, IntValue, Metadata>> tables1) {
            for (String name : tables0.keySet()) {
                Table<StringKey, IntValue, Metadata> table = tables0.get(name);
                Table<StringKey, IntValue, Metadata> mapKeys = tables1.get(name);

                assertThat(table.count() == mapKeys.count()).isTrue();

                try (TxnContext txn = srcCorfuStore.txn(TEST_NAMESPACE)) {
                    table.entryStream().forEachOrdered(dstEntry -> {
                        StringKey tableKey = dstEntry.getKey();
                        CorfuStoreEntry<StringKey, IntValue, Metadata> srcEntry = txn.getRecord(table, tableKey);
                        assertThat(dstEntry.getPayload().getValue()).isEqualTo(srcEntry.getPayload().getValue());
                    });
                    txn.commit();
                }
            }
    }

    /**
     * Wait replication data reach at the sink cluster.
     * @param tables
     * @param hashMap
     */
    private void waitData(Map<String, Table<StringKey, IntValue, Metadata>> tables,
                          Map<String, Map<String, Integer>> hashMap) {
        for (String name : hashMap.keySet()) {
            Table<StringKey, IntValue, Metadata> table = tables.get(name);
            Map<String, Integer> mapKeys = hashMap.get(name);
            while (table.count() < mapKeys.size()) {
                log.trace("table count: {}, mapKeys size: {}", table.count(), mapKeys.size());
            }
        }
    }

    private void verifyData(CorfuStore corfuStore, Map<String, Table<StringKey, IntValue, Metadata>> tables,
                            Map<String, Map<String, Integer>> hashMap) {
        for (String name : hashMap.keySet()) {
            Table<StringKey, IntValue, Metadata> table = tables.get(name);
            Map<String, Integer> mapKeys = hashMap.get(name);

            log.debug("Table[" + name + "]: " + table.count() + " keys; Expected "
                    + mapKeys.size() + " keys");

            assertThat(table.count()).isEqualTo(mapKeys.size());
            try (TxnContext txn = corfuStore.txn(TEST_NAMESPACE)) {
                for (String key : mapKeys.keySet()) {
                    StringKey tableKey = StringKey.newBuilder().setKey(key).build();
                    CorfuStoreEntry<StringKey, IntValue, Metadata> entry = txn.getRecord(table, tableKey);
                    assertThat(entry.getPayload().getValue()).isEqualTo(mapKeys.get(key));
                }
                txn.commit();
            }
        }
    }

    private void verifyNoData(Map<String, Table<StringKey, IntValue, Metadata>> tables) {
        for (Table table : tables.values()) {
            assertThat(table.count()).isEqualTo(0);
        }
    }

    /* ***************************** LOG REPLICATION IT TESTS ***************************** */

    /**
     * This test attempts to perform a snapshot sync and log entry sync through the Log Replication Manager.
     * We emulate the channel between source and destination by directly handling the received data
     * to the other side.
     */
    @Test
    public void testSnapshotAndLogEntrySyncThroughManager() throws Exception {
        testSnapshotSyncAndLogEntrySync(0, false, 0);
    }

    /**
     * In this test we emulate the following scenario, 3 tables (T0, T1, T2). Only T0 and T1 are replicated.
     * We write following this pattern:
     *
     * - Write transactions across T0 and T1.
     * - Write individual transactions for T0 and T1
     * - Write transactions to T2
     *
     *
     * @throws Exception
     */
    @Test
    public void testValidSnapshotSyncCrossTables() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0NameUFO);
        crossTables.add(t1NameUFO);

        writeCrossTableTransactions(crossTables, true);
        // Open table2 with false is_federated flag to prevent it from being replicated.
        Table<StringKey, IntValue, Metadata> table = srcCorfuStore.openTable(
                TEST_NAMESPACE,
                t2Name,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(Sample.IntValue.class)
        );
        srcCorfuTables.put(table.getFullyQualifiedTableName(), table);

        // Start Snapshot Sync
        startSnapshotSync(Collections.singleton(WAIT.ON_METADATA_RESPONSE));

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");

        //verify isDataConsistent is true
        sourceDataSender.checkStatusOnSink(true);

        // Because t2 should not have been replicated remove from expected list
        srcDataForVerification.get(t2NameUFO).clear();
        verifyData(dstCorfuStore, dstCorfuTables, srcDataForVerification);

        cleanEnv();
    }

    /**
     * In this test we emulate the following scenario, 3 tables (T0, T1, T2). Only T0 and T1 are replicated,
     * however, transactions are written across the 3 tables.
     *
     * This scenario succeeds, as snapshot sync does not rely on the transaction stream and thus is able to
     * replicate T0 and T1 independently.
     *
     * @throws Exception
     */
    @Test
    public void testInvalidSnapshotSyncCrossTables() throws Exception {
        // Write data in transaction to t0, t1 and t2 (where t2 is not intended to be replicated)
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0NameUFO);
        crossTables.add(t1NameUFO);
        crossTables.add(t2NameUFO);

        // Replicate Tables
        Set<String> replicateTables = new HashSet<>();
        replicateTables.add(t0NameUFO);
        replicateTables.add(t1NameUFO);

        writeCrossTableTransactions(crossTables, true);
        // Open table2 with false is_federated flag to prevent it from being replicated.
        Table<StringKey, IntValue, Metadata> table = srcCorfuStore.openTable(
                TEST_NAMESPACE,
                t2Name,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(Sample.IntValue.class)
        );
        srcCorfuTables.put(table.getFullyQualifiedTableName(), table);

        // Start Snapshot Sync
        startSnapshotSync(Collections.singleton(WAIT.ON_METADATA_RESPONSE));

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");

        //verify isDataConsistent is true
        sourceDataSender.checkStatusOnSink(true);
        // Because t2 should not have been replicated remove from expected list
        srcDataForVerification.get(t2NameUFO).clear();
        verifyData(dstCorfuStore, dstCorfuTables, srcDataForVerification);
        cleanEnv();
    }

    /**
     * Test behaviour when snapshot sync is initiated and the log is empty.
     * The Snapshot Sync should immediately complete and no data should appear at destination.
     */
    @Test
    public void testSnapshotSyncForEmptyLog() throws Exception {
        // Setup Environment
        setupEnv();

        // Open Streams on Source
        openStreams(srcCorfuTables, srcCorfuStore, NUM_STREAMS);

        // Verify no data on source
        log.debug("****** Verify No Data in Source Site");
        verifyNoData(srcCorfuTables);

        // Verify destination tables have no actual data.
        openStreams(dstCorfuTables, dstCorfuStore, NUM_STREAMS);
        log.debug("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);

        // Tables to Replicate
        Set<String> tablesToReplicate = new HashSet<>();
        tablesToReplicate.add(t0NameUFO);
        tablesToReplicate.add(t1NameUFO);

        // We don't write data to the log
        // StartSnapshotSync (no actual data present in the log)
        startSnapshotSync(Collections.singleton(WAIT.ON_METADATA_RESPONSE));

        // Verify No Data On Destination
        verifyNoData(dstCorfuTables);

        cleanEnv();
    }

    /**
     * Test behaviour when snapshot sync is initiated and even though the log is not empty, there
     * is no actual data for the streams to replicate.
     * The Snapshot Sync should immediately complete and no data should appear at destination.
     */
    @Test
    public void testSnapshotSyncForNoData() throws Exception {
        // Setup Environment
        setupEnv();

        // Generate transactional data across t0, t1 and t2
        openStreams(srcCorfuTables, srcCorfuStore, TOTAL_STREAM_COUNT);

        // Verify data on source is actually present
        log.debug("****** Verify Data in Source Site");
        verifyData(srcCorfuStore, srcCorfuTables, srcDataForVerification);

        // Verify destination tables have no actual data before log replication
        openStreams(dstCorfuTables, dstCorfuStore, NUM_STREAMS);
        log.debug("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);

        // We don't write data to the log
        // StartSnapshotSync (no actual data present in the log)
        startSnapshotSync(new HashSet<>());

        // Verify No Data On Destination
        verifyNoData(dstCorfuTables);
        cleanEnv();
    }

    /**
     * Test behaviour when log entry sync is initiated and the log is empty. It should continue
     * attempting log entry sync indefinitely without any error.
     */
    @Test
    public void testLogEntrySyncForEmptyLog() throws Exception {
        // Setup Environment
        setupEnv();

        // Open Streams on Source
        openStreams(srcCorfuTables, srcCorfuStore, NUM_STREAMS);

        // Verify no data on source
        log.debug("****** Verify No Data in Source Site");
        verifyNoData(srcCorfuTables);

        // Verify destination tables have no actual data.
        openStreams(dstCorfuTables, dstCorfuStore, NUM_STREAMS);
        log.debug("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);

        // We did not write data to the log
        testConfig.clear();
        LogReplicationSourceManager sourceManager = startSnapshotSync(Collections.singleton(WAIT.NONE));

        // Wait until Log Entry Starts
        checkStateChange(sourceManager.getLogReplicationFSM(), LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);

        int retries = 0;
        // Check for several iterations that it is still in Log Entry Sync State and no Error because of empty log
        // shutdown the state machine
        while (retries < STATE_CHANGE_CHECKS) {
            checkStateChange(sourceManager.getLogReplicationFSM(), LogReplicationStateType.IN_LOG_ENTRY_SYNC, false);
            retries++;
            sleep(WAIT_STATE_CHANGE);
        }

        // Verify No Data On Destination
        verifyNoData(dstCorfuTables);

        cleanEnv();
    }

    /**
     * Test Log Entry (delta) Sync for the case where messages are dropped at the destination
     * for a fixed number of times. This will test messages being resent and further applied.
     */
    @Test
    public void testLogEntrySyncValidCrossTablesWithDropMsg() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0NameUFO);
        crossTables.add(t1NameUFO);

        writeCrossTableTransactions(crossTables, true);
        // Open table2 with false is_federated flag to prevent it from being replicated.
        Table<StringKey, IntValue, Metadata> table = srcCorfuStore.openTable(
                TEST_NAMESPACE,
                t2Name,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(Sample.IntValue.class)
        );
        srcCorfuTables.put(table.getFullyQualifiedTableName(), table);

        LogReplicationSourceManager sourceManager = startSnapshotSync(new HashSet<>());
        checkStateChange(sourceManager.getLogReplicationFSM(), LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;

        testConfig.clear().setDropMessageLevel(1);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");
        srcDataForVerification.get(t2NameUFO).clear();
        // Verify Destination
        verifyData(dstCorfuStore, dstCorfuTables, srcDataForVerification);
    }

    /**
     * Test Log Entry Sync, when the first transaction encountered
     * writes data across replicated and non-replicated streams.
     *
     * This test should succeed as we filter the streams of interest, and limit replication to this subset.
     *
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncInvalidCrossTables() throws Exception {
        testLogEntrySyncCrossTableTransactions(true);
    }

    /**
     * Test Log Entry Sync, when transactions are performed across replicated and non-replicated tables
     * (i.e., NOT all tables in the transaction are set to be replicated).
     *
     * This test should succeed log replication completely as we do support
     * transactions across federated and non-federated tables.
     *
     * In this test, we first initiate transactions on valid replicated streams, and then introduce
     * transactions across replicated and non-replicated tables, we verify log entry sync is
     * achieved fully.
     *
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncInvalidCrossTablesPartial() throws Exception {
        testLogEntrySyncCrossTableTransactions(false);
    }

    private void testLogEntrySyncCrossTableTransactions(boolean startWithCrossTableTxs) throws Exception {
        // Write data in transaction to t0, t1 (tables to be replicated) and also include a non-replicated table
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0NameUFO);
        crossTables.add(t1NameUFO);
        crossTables.add(t2NameUFO);

        // Writes transactions to t0, t1 and t2 + transactions across 'crossTables'
        writeCrossTableTransactions(crossTables, startWithCrossTableTxs);

        // Start Log Entry Sync
        testConfig.clear();

        LogReplicationFSM fsm = startLogEntrySync(Collections.singleton(WAIT.ON_ACK), true, null);

        checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");

        // Verify Destination
        verifyData(dstCorfuStore, dstCorfuTables, srcDataForVerification);
    }

    /**
     * While replication log entries from src to dst, still continue to pump data at the src with transactions
     * The transactions will include both put and delete entries.
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncValidCrossTablesWithWritingAtSrc() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0NameUFO);
        crossTables.add(t1NameUFO);

        writeCrossTableTransactions(crossTables, true);

        testConfig.clear();
        testConfig.setWritingSrc(true);
        testConfig.setDeleteOP(true);
        testConfig.setWaitOn(WAIT.ON_ACK);

        startLogEntrySync(Collections.singleton(WAIT.ON_ACK), false, null);

        // Verify Data on Destination site
        log.debug("****** Wait Data on Destination");
        waitData(dstCorfuTables, srcDataForVerification);

        log.debug("****** Verify Data on Destination");
        // Verify Data on Destination
        verifyData(dstCorfuStore, dstCorfuTables, srcDataForVerification);

        // expectedAckTimestamp was set in 'startLogEntrySync' to the tail of the Log Replication Stream.  Verify
        // that the metadata table was updated with it after a successful LogEntrySync
        assertThat(expectedAckTimestamp.get()).isEqualTo(logReplicationMetadataManager.getLastProcessedLogEntryBatchTimestamp());
        verifyPersistedSnapshotMetadata();
        verifyPersistedLogEntryMetadata();

        cleanEnv();
    }

    /**
     * This test attempts to perform a snapshot sync, but the log is trimmed in the middle of the process.
     *
     * Because data is loaded in memory by the snapshot logreader on initial access, the log
     * replication will not perceive the Trimmed Exception and it should complete successfully.
     */
    @Test
    public void testSnapshotSyncWithTrimmedExceptions() throws Exception {
        final int RX_MESSAGES_LIMIT = 2;

        // Setup Environment: two corfu servers (source & destination)
        setupEnv();

        // Open One Stream
        openStreams(srcCorfuTables, srcCorfuStore, 1);
        openStreams(dstCorfuTables, dstCorfuStore, 1);

        // Let's generate data on Source Corfu Server to be Replicated
        // Write a very large number of entries, so we can be sure the trim happens during snapshot sync
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS_LARGE, srcCorfuStore, 0);

        // Replicate the only table we created, block until 2 messages are received,
        // then enforce a trim on the log.
        expectedSinkReceivedMessages = RX_MESSAGES_LIMIT;
        expectedAckTimestamp.set(-1);
        testConfig.setWaitOn(WAIT.ON_ACK_TS);

        LogReplicationSourceManager sourceManager = startSnapshotSync(new HashSet<>(Arrays.asList(WAIT.ON_ACK,
                WAIT.ON_ACK_TS, WAIT.ON_ERROR, WAIT.ON_SINK_RECEIVE)));

        // KWrite a checkpoint and trim
        Token token = checkpointAndTrimCorfuStore(srcTestRuntime);
        srcDataRuntime.getAddressSpaceView().invalidateServerCaches();
        expectedAckTimestamp.set(srcDataRuntime.getAddressSpaceView().getLogTail());

        log.debug("\n****** Wait until an Trimmed Error happens");
        blockUntilExpectedValueReached.acquire();
        log.debug("\n****** Got an expected error");

        // Be sure log was trimmed
        while (srcDataRuntime.getAddressSpaceView().getTrimMark().getSequence()
                < token.getSequence()) {
            // no-op
        }

        // Block until snapshot sync is completed (ack is received)
        log.debug("\n****** Wait until snapshot sync is completed (ack received)");
        //blockUntilExpectedValueReached.acquire();
        blockUntilExpectedAckType.acquire();

        // Verify its in log entry sync state and that data was completely transferred to destination
        checkStateChange(sourceManager.getLogReplicationFSM(), LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);
        verifyTables(dstCorfuTables, srcCorfuTables);
    }

    /**
     * This test attempts to perform a Log Entry sync, but the log is trimmed in the middle of the process.
     *
     * Because data is loaded in memory by the log entry logreader on initial access, the log
     * replication will not perceive the Trimmed Exception and it should complete successfully.
     */
    @Test
    public void testLogEntrySyncWithTrim() throws Exception {
        final int RX_MESSAGES_LIMIT = 2;

        // Setup Environment: two corfu servers (source & destination)
        setupEnv();

        // Open One Stream
        openStreams(srcCorfuTables, srcCorfuStore, 1);
        openStreams(dstCorfuTables, dstCorfuStore, 1);

        // Let's generate data on Source Corfu Server to be Replicated
        // Write a large number of entries, so we can be sure the trim happens during log entry sync
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS_LARGE, srcCorfuStore, 0);

        // Replicate the only table we created, block until 2 messages are received,
        // then enforce a trim on the log.
        expectedSinkReceivedMessages = RX_MESSAGES_LIMIT;

        LogReplicationFSM fsm = startLogEntrySync(Collections.singleton(WAIT.ON_SINK_RECEIVE), false, null);

        log.debug("****** Trim log, will trigger a snapshot sync");

        Token toke = checkpointAndTrimCorfuStore(srcDataRuntime);

        // Be sure log was trimmed
        while (srcDataRuntime.getAddressSpaceView().getTrimMark().getSequence() < toke.getSequence()) {
            // no-op
        }

        log.debug("****** Wait until IN_SNAPSHOT_SYNC");
        checkStateChange(fsm, LogReplicationStateType.IN_SNAPSHOT_SYNC, true);

        log.debug("Block until full snapshot transfer complete");
        checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);

        verifyData(dstCorfuStore, dstCorfuTables, dstDataForVerification);
        cleanEnv();
    }

    /**
     * Start log entry sync for large table (20K keys). This allows to determine
     * if the Sink Ack ratio is correct and does not timeout on the Source.
     *
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncLargeTables() throws Exception {
        // Setup Environment: two corfu servers (source & destination)
        setupEnv();

        // Open One Stream
        openStreams(srcCorfuTables, srcCorfuStore, 1);
        openStreams(dstCorfuTables, dstCorfuStore, 1);

        int num_keys = NUM_KEYS_VERY_LARGE;

        // Let's generate data on Source Corfu Server to be Replicated
        // Write a very large number of entries, so we can be sure the trim happens during snapshot sync
        log.debug("****** Generate TX Data");
        generateTXData(srcCorfuTables, srcDataForVerification, num_keys, srcCorfuStore, 0);

        // Replicate the only table we created, block until 10 messages are received,
        // then enforce a trim on the log.
        log.debug("****** Start Log Entry Sync");
        expectedAckMessages = num_keys;
        LogReplicationFSM fsm = startLogEntrySync(Collections.singleton(WAIT.ON_ACK), false, null);

        log.debug("****** Total " + num_keys + " ACK messages received. Verify State.");

        // Verify its in log entry sync state and that data was completely transferred to destination
        checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);
        verifyData(dstCorfuStore, dstCorfuTables, dstDataForVerification);
        cleanEnv();
    }

    /**
     * Test the case where a Snapshot Sync apply phase takes several cycles to complete.
     * We need to ensure the Sender re-schedules verification of snapshot sync status and
     * once completed moves onto Log Entry Sync.
     */
    @Test
    public void testSnapshotSyncLongDurationApply() throws Exception {
        final int numCyclesToDelayApply = 3;
        testSnapshotSyncAndLogEntrySync(numCyclesToDelayApply, false, 0);
    }

    /**
     * Test the case where a Snapshot Sync apply metadata response is delayed causing TimeoutExceptions.
     * Verify we are able to recover.
     */
    @Test
    public void testSnapshotSyncDelayedApplyResponse() throws Exception {
        testSnapshotSyncAndLogEntrySync(0, true, 0);
    }

    private void testSnapshotSyncAndLogEntrySync(int numCyclesToDelayApply, boolean delayResponse, int dropAcksLevel) throws Exception {
        // Setup two separate Corfu Servers: source (source) and destination (sink)
        setupEnv();

        // Open streams in source Corfu
        openStreams(srcCorfuTables, srcCorfuStore, NUM_STREAMS);

        // Write data into Source Tables
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS, srcCorfuStore, NUM_KEYS);

        // Verify data just written against in-memory copy
        log.debug("****** Verify Source Data");
        verifyData(srcCorfuStore, srcCorfuTables, srcDataForVerification);

        // Before initiating log replication, verify these tables have no actual data in the destination.
        openStreams(dstCorfuTables, dstCorfuStore, NUM_STREAMS);
        log.debug("****** Verify No Data in Destination");
        verifyNoData(dstCorfuTables);

        expectedAckTimestamp.set(Long.MAX_VALUE);
        testConfig.setDelayedApplyCycles(numCyclesToDelayApply);
        testConfig.setTimeoutMetadataResponse(delayResponse);
        testConfig.setDropAckLevel(dropAcksLevel);

        // Start Snapshot Sync (through Source Manager)
        Set<WAIT> conditions = new HashSet<>();
        conditions.add(WAIT.ON_METADATA_RESPONSE); // Condition to wait for snapshot sync
        conditions.add(WAIT.ON_ACK_TS); // Condition to wait for log entry sync
        startSnapshotSync(conditions);

        log.debug("****** Snapshot Sync COMPLETE");

        //verify isDataConsistent is true
        sourceDataSender.checkStatusOnSink(true);

        testConfig.setWaitOn(WAIT.ON_ACK_TS);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");
        verifyData(dstCorfuStore, dstCorfuTables, srcDataForVerification);

        blockUntilExpectedAckTs.acquire();
        expectedAckTimestamp.set(srcDataRuntime.getAddressSpaceView().getLogTail() + NUM_KEYS_LARGE);

        // Write Extra Data (for incremental / log entry sync)
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS_LARGE, srcCorfuStore, NUM_KEYS*2);

        // Block until the log entry sync completes == expected number of ACKs are received
        log.debug("****** Wait until log entry sync completes and ACKs are received");
        blockUntilExpectedAckTs.acquire();

        // Verify Data at Destination
        log.debug("****** Verify Destination Data for log entry (incremental updates)");
        verifyData(dstCorfuStore, dstCorfuTables, srcDataForVerification);

        verifyPersistedSnapshotMetadata();
        verifyPersistedLogEntryMetadata();
    }


    /**
     * Test Log Entry (delta) Sync for the case where the ACKs are arbitrarily dropped
     * for a fixed number of times at the Source. This will test that LR is
     * (i) not impacted by dropped ACKs
     * (ii) Source resends msgs for which it hasn't received the ACKs
     * (iii) Sink handles the already seen and processed msgs.
     */
    @Test
    public void testLogEntrySyncWithAckDrops() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0NameUFO);
        crossTables.add(t1NameUFO);

        // Writes transactions to t0, t1 and t2 + transactions across 'crossTables'
        writeCrossTableTransactions(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages = NUM_KEYS * WRITE_CYCLES;
        testConfig.clear().setDropAckLevel(1);

        startLogEntrySync(Collections.singleton(WAIT.ON_ACK), true, null);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");

        // Verify Destination
        verifyData(dstCorfuStore, dstCorfuTables, srcDataForVerification);
        cleanEnv();
    }


    /**
     * Test Log Entry (delta) Sync for the case where messages are arbitrarily dropped at the
     * destination and ACKs are arbitrarily dropped at the source. This tests
     * (i) messages dropped at the destination is resent. This would also test that Sink handles out of order messages.
     * (ii) messages for which ACKs were dropped at the source are resent and Sink handles already processed data.
     */
    @Test
    public void testLogEntrySyncWithMsgDropsAndAckDrops() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0NameUFO);
        crossTables.add(t1NameUFO);

        writeCrossTableTransactions(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;

        testConfig.clear().setDropMessageLevel(1);
        testConfig.setDropAckLevel(1);

        startLogEntrySync(Collections.singleton(WAIT.ON_ACK), true, null);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");

        // Verify Destination
        verifyData(dstCorfuStore, dstCorfuTables, srcDataForVerification);
        cleanEnv();
    }

    /**
     *  Test Log_Entry (delta) Sync for the case where an ACK gets dropped at the Source, and
     *  immediately, the Source gets REPLICATION_STOP event. We then emulate negotiation and overall
     *  FSM transitions from LOG_ENTRY SYNC -> INITIALIZED (Negotiation happens around here) -> LOG_ENTRY SYCN.
     *  This tests that on replication_stop event, the pending queue is cleared. This ensures that msgs are not
     *  resent after the REPLICATION_STOP event, and the data is replicated after the last run of log_entry sync
     *
     *  Also, tests that an ACK is always sent for all msg resends, and also,
     *  if a msg is ignored by Sink for any reason, the ACK sent by Sink should not be for the ignored msg
     **/
    @Test
    public void testLogEntrySyncWithFSMChangeAndWithAckDrop() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0NameUFO);
        crossTables.add(t1NameUFO);

        // Writes transactions to t0, t1 and t2 + transactions across 'crossTables'
        writeCrossTableTransactions(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages = NUM_KEYS * WRITE_CYCLES;
        testConfig.clear().setDropAckLevel(2);

        Set<WAIT> waitCondition = new HashSet<>();
        waitCondition.add(WAIT.NONE);
        startLogEntrySync(waitCondition, true, this::changeState);

        blockUntilFSMTransition.await();

        checkStateChange(logReplicationSourceManager.getLogReplicationFSM(),
                LogReplicationStateType.INITIALIZED, true);
        testConfig.clear();

        // add a listner to ACKs received. This is used to unblock the current thread before the final verification.
        ackMessages = sourceDataSender.getAckMessages();
        ackMessages.addObserver(this);

        // simulate negotiation. Return metadata from the sink
        LogReplicationMetadataResponseMsg negotiationResponse = sourceDataSender.getSinkManager()
                .getLogReplicationMetadataManager()
                .getMetadataResponse(CorfuMessage.HeaderMsg.newBuilder().build())
                .getPayload().getLrMetadataResponse();

        logReplicationSourceManager.getLogReplicationFSM().input(
                new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST,
                        new LogReplicationEventMetadata(LogReplicationEventMetadata.getNIL_UUID(),
                                negotiationResponse.getLastLogEntryTimestamp(), negotiationResponse.getSnapshotApplied())));
        checkStateChange(logReplicationSourceManager.getLogReplicationFSM(),
                LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);

        sourceDataSender.resetTestConfig(testConfig);

        // Write more data to source side in case all the acks have been handled before blockUntilExpectedAckTs is released.
        generateTransactionsCrossTables(srcCorfuTables, Collections.singleton(t0NameUFO), srcDataForVerification, NUM_KEYS, srcCorfuStore, NUM_KEYS);
        expectedAckTimestamp.set(srcDataRuntime.getAddressSpaceView().getLogTail());

        // Block until the expected ACK Timestamp is reached
        blockUntilExpectedAckTs.acquire();

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");

        // Verify Destination
        verifyData(dstCorfuStore, dstCorfuTables, srcDataForVerification);
        cleanEnv();
    }


    @Test
    public void testSnapshotSyncWithAckDrops() throws Exception {
        testSnapshotSyncAndLogEntrySync(0, false, 1);
        cleanEnv();
    }



    /* ********************** AUXILIARY METHODS ********************** */

    // startCrossTx indicates if we start with a transaction across Tables
    private void writeCrossTableTransactions(Set<String> tableNames, boolean startCrossTx) throws Exception {
        // Setup two separate Corfu Servers: source (primary) and destination (sink)
        setupEnv();

        // Open streams in source Corfu
        int totalStreams = TOTAL_STREAM_COUNT; // test0, test1, test2 (open stream tables)
        openStreams(srcCorfuTables, srcCorfuStore, totalStreams);

        // Write data across to tables specified in crossTableTransactions in transaction
        if (startCrossTx) {
            generateTransactionsCrossTables(srcCorfuTables, tableNames, srcDataForVerification, NUM_KEYS, srcCorfuStore, 0);
        }

        // Write data to t0
        generateTransactionsCrossTables(srcCorfuTables, Collections.singleton(t0NameUFO), srcDataForVerification, NUM_KEYS, srcCorfuStore, NUM_KEYS);

        // Write data to t1
        generateTransactionsCrossTables(srcCorfuTables, Collections.singleton(t1NameUFO), srcDataForVerification, NUM_KEYS, srcCorfuStore, NUM_KEYS);

        // Write data to t2
        generateTransactionsCrossTables(srcCorfuTables, Collections.singleton(t2NameUFO), srcDataForVerification, NUM_KEYS, srcCorfuStore, 0);

        // Write data in tables specified in tableNames
        generateTransactionsCrossTables(srcCorfuTables, tableNames, srcDataForVerification, NUM_KEYS, srcCorfuStore,
            NUM_KEYS*2);

        // Verify data just written against in-memory copy
        verifyData(srcCorfuStore, srcCorfuTables, srcDataForVerification);

        // Before initiating log replication, verify these tables have no actual data in the destination node.
        openStreams(dstCorfuTables, dstCorfuStore, totalStreams);
        log.debug("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);
    }

    private void startTxAtSrc() {
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0NameUFO);
        crossTables.add(t1NameUFO);

        generateTransactionsCrossTables(srcCorfuTables, crossTables, srcDataForVerification,
                NUM_KEYS_LARGE, srcCorfuStore, NUM_KEYS*WRITE_CYCLES);
    }

    private void trim(CorfuRuntime rt, int trimAddress) {
        log.debug("Trim at: " + trimAddress);
        rt.getAddressSpaceView().prefixTrim(new Token(0, trimAddress));
        rt.getAddressSpaceView().invalidateServerCaches();
    }

    /**
     * It will start to continue pump data to src or dst according to the config.
     */
    private void startTx() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
        if (testConfig.writingSrc) {
            expectedAckMessages += NUM_KEYS_LARGE;
            scheduledExecutorService.submit(this::startTxAtSrc);
        }
    }

    private LogReplicationSourceManager startSnapshotSync(Set<WAIT> waitConditions) throws Exception {

        // Observe metadata responses coming from receiver, until it indicates snapshot sync apply has completed
        blockUntilExpectedMetadataResponse.acquire();

        logReplicationSourceManager = setupSourceManagerAndObservedValues(waitConditions, null);

        // Start Snapshot Sync
        log.debug("****** Start Snapshot Sync");
        logReplicationSourceManager.startSnapshotSync();

        // Block until the wait condition is met (triggered)
        log.debug("****** Wait until wait condition is met {}", waitConditions);

        if (waitConditions.contains(WAIT.ON_ERROR)) {
            log.debug("****** blockUntilExpectedValueReached {}", expectedErrors);
            blockUntilExpectedValueReached.acquire();
        } else if (waitConditions.contains(WAIT.ON_METADATA_RESPONSE)) {
            log.debug("****** Block until metadata response indicating snapshot sync apply is completed is received.");
            blockUntilExpectedMetadataResponse.acquire();
        } else {
            log.debug("****** blockUntilExpectedAckType {}", expectedAckMsgType);
            blockUntilExpectedAckType.acquire();
        }

        return logReplicationSourceManager;
    }

    private LogReplicationFSM startLogEntrySync(Set<WAIT> waitConditions,
                                                boolean injectTxData, TransitionSource function) throws Exception {

        logReplicationSourceManager = setupSourceManagerAndObservedValues(waitConditions, function);

        // Start Log Entry Sync
        log.info("****** Start Log Entry Sync with src tail " + srcDataRuntime.getAddressSpaceView().getLogTail()
                + " dst tail " + dstDataRuntime.getAddressSpaceView().getLogTail());
        logReplicationSourceManager.startReplication(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST,
                new LogReplicationEventMetadata(UUID.randomUUID(), -1, -1)));

        // Start TX's in parallel, while log entry sync is running
        if (injectTxData) {
            startTx();
        }

        // We need to block until the ack for the last address in LogReplication stream is received
        expectedAckMessages = Utils.getLogAddressSpace(srcDataRuntime
            .getLayoutView().getRuntimeLayout())
            .getAddressMap()
            .get(ObjectsView.getLogReplicatorStreamId()).getTail();

        blockUntilExpectedAckTs.acquire();
        expectedAckTimestamp.set(expectedAckMessages);

        // Block until the expected ACK Timestamp is reached
        log.debug("****** Wait until the wait condition is met");
        if (waitConditions.contains(WAIT.ON_ERROR) || waitConditions.contains(WAIT.ON_TIMEOUT_ERROR)) {
            blockUntilExpectedValueReached.acquire();
        } else if (waitConditions.contains(WAIT.ON_ACK)) {
            blockUntilExpectedAckTs.acquire();
        }
        return logReplicationSourceManager.getLogReplicationFSM();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private LogReplicationSourceManager setupSourceManagerAndObservedValues(Set<WAIT> waitConditions,
        TransitionSource function) throws InterruptedException {

        LogReplicationConfigManager configManager = new LogReplicationConfigManager(srcTestRuntime);
        LogReplicationUpgradeManager upgradeManager = new LogReplicationUpgradeManager(srcTestRuntime, nettyConfig);

        // This IT requires custom values to be set for the replication config.  Set these values so that the default
        // values are not used
        configManager.getConfig().setMaxNumMsgPerBatch(BATCH_SIZE);
        configManager.getConfig().setMaxMsgSize(SMALL_MSG_SIZE);
        configManager.getConfig().setMaxDataSizePerMsg(SMALL_MSG_SIZE * LogReplicationConfig.DATA_FRACTION_PER_MSG / 100);

        LogReplicationContext replicationContext = new LogReplicationContext(configManager, 0, DEFAULT_ENDPOINT);

        // Data Sender
        sourceDataSender = new SourceForwardingDataSender(DESTINATION_ENDPOINT, replicationContext, testConfig,
            logReplicationMetadataManager, nettyConfig, function);

        ReplicationSession replicationSession =
            ReplicationSession.getDefaultReplicationSessionForCluster(REMOTE_CLUSTER_ID);

        // Source Manager
        LogReplicationSourceManager logReplicationSourceManager = new LogReplicationSourceManager(
            LogReplicationRuntimeParameters.builder().remoteClusterDescriptor(new ClusterDescriptor(REMOTE_CLUSTER_ID,
                LogReplicationClusterInfo.ClusterRole.SOURCE, CORFU_PORT)).replicationConfig(configManager.getConfig())
                .localCorfuEndpoint(SOURCE_ENDPOINT).build(), logReplicationMetadataManager, sourceDataSender,
                replicationContext, upgradeManager, replicationSession);

        // Set Log Replication Source Manager so we can emulate the channel for data & control messages (required
        // for testing)
        sourceDataSender.setSourceManager(logReplicationSourceManager);

        // Add this class as observer of the value of interest for the wait condition
        for (WAIT waitCondition : waitConditions) {
            if (waitCondition == WAIT.ON_ACK || waitCondition == WAIT.ON_ACK_TS) {
                ackMessages = sourceDataSender.getAckMessages();
                ackMessages.addObserver(this);
            } else if (waitCondition == WAIT.ON_ERROR || waitCondition == WAIT.ON_TIMEOUT_ERROR) {
                // Wait on Error Notifications to Source
                errorsLogEntrySync = sourceDataSender.getErrors();
                errorsLogEntrySync.addObserver(this);
            } else if (waitCondition == WAIT.ON_METADATA_RESPONSE) {
                metadataResponseObservable = sourceDataSender.getMetadataResponses();
                metadataResponseObservable.addObserver(this);
                // Wait on Received Messages on Sink (Destination)
                sinkReceivedMessages = sourceDataSender.getSinkManager().getRxMessageCount();
                sinkReceivedMessages.addObserver(this);
            } else if (waitCondition == WAIT.ON_SINK_RECEIVE) {
                // Wait on Received Messages on Sink (Destination)
                sinkReceivedMessages = sourceDataSender.getSinkManager().getRxMessageCount();
                sinkReceivedMessages.addObserver(this);
            }
        }

        if (!waitConditions.contains(WAIT.NONE)) {
            // Acquire semaphore for the first time, so next time it blocks until the observed
            // value reaches the expected value.
            blockUntilExpectedValueReached.acquire();
        }

        return logReplicationSourceManager;
    }


    /**
     * Check that the FSM has changed state. If 'waitUntil' flag is set, we will wait until the state reaches
     * the expected 'finalState'
     */
    private void checkStateChange(LogReplicationFSM fsm, LogReplicationStateType finalState, boolean waitUntil) {
        // Due to the error, verify FSM has moved to the stopped state, as this kind of error should terminate
        // the state machine execution.
        while (fsm.getState().getType() != finalState && waitUntil) {
            // Wait until state changes
        }

        assertThat(fsm.getState().getType()).isEqualTo(finalState);
    }

    // Callback for observed values
    @Override
    public void update(Observable o, Object arg) {
        if (o.equals(ackMessages)) {
            verifyExpectedAckMessage((ObservableAckMsg)o);
        } else if (o.equals(errorsLogEntrySync)) {
            verifyExpectedValue(expectedErrors, (int)errorsLogEntrySync.getValue());
        } else if (o.equals(sinkReceivedMessages)) {
            verifyExpectedValue(expectedSinkReceivedMessages, (int)sinkReceivedMessages.getValue());
        } else if (o.equals(metadataResponseObservable)) {
            verifyMetadataResponse(metadataResponseObservable.getValue());
        }
    }

    private void verifyMetadataResponse(LogReplicationMetadataResponseMsg response) {
        if (response.getSnapshotTransferred() == response.getSnapshotApplied()) {
            log.debug("Metadata response indicates snapshot sync apply has completed");
            blockUntilExpectedMetadataResponse.release();
        } else {
            log.debug("Metadata response indicates snapshot sync apply is still in progress, transferred={}, applied={}", response.getSnapshotTransferred(),
                    response.getSnapshotApplied());
        }
    }

    private void verifyExpectedValue(long expectedValue, long currentValue) {
        // If expected value, release semaphore / unblock the wait
        if (expectedValue == currentValue && expectedAckMsgType != null) {
            blockUntilExpectedValueReached.release();
        }
    }

    private void verifyExpectedAckMessage(ObservableAckMsg observableAckMsg) {
        // If expected a ackTs, release semaphore / unblock the wait
        if (observableAckMsg.getDataMessage() != null) {
            LogReplicationEntryMsg logReplicationEntry = observableAckMsg.getDataMessage();

            if (testConfig.waitOn == WAIT.ON_ACK) {
                verifyExpectedValue(expectedAckMessages, ackMessages.getMsgCnt());
            }

            if (testConfig.waitOn == WAIT.ON_ACK || testConfig.waitOn == WAIT.ON_ACK_TS) {
                verifyExpectedValue(expectedAckTimestamp.get(), logReplicationEntry.getMetadata().getTimestamp());
                if (expectedAckMsgType == logReplicationEntry.getMetadata().getEntryType()) {
                    blockUntilExpectedAckType.release();
                }

                log.debug("expectedAckTs={}, logEntryTs={}", expectedAckTimestamp.get(),
                    logReplicationEntry.getMetadata().getTimestamp());

                if (expectedAckTimestamp.get() == logReplicationEntry.getMetadata().getTimestamp()) {
                    blockUntilExpectedAckTs.release();
                }
            }
        }
    }

    private void verifyPersistedSnapshotMetadata() {
        long lastSnapshotStart = logReplicationMetadataManager.getLastStartedSnapshotTimestamp();
        long lastSnapshotDone = logReplicationMetadataManager.getLastAppliedSnapshotTimestamp();
        assertThat(lastSnapshotStart).isEqualTo(lastSnapshotDone);
    }

    private void verifyPersistedLogEntryMetadata() {
        long lastLogProcessed = logReplicationMetadataManager.getLastProcessedLogEntryBatchTimestamp();

        log.debug("\nlastLogProcessed " + lastLogProcessed + " expectedTimestamp " + expectedAckTimestamp.get());
        assertThat(expectedAckTimestamp.get() == lastLogProcessed).isTrue();
    }

    private void changeState() {
        assertThat(sourceDataSender.getAckMessages().getDataMessage()).isNotNull();
        LogReplicationEntryMsg ack = sourceDataSender.getAckMessages().getDataMessage();

        logReplicationSourceManager.getLogReplicationFSM().input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_STOP,
                new LogReplicationEventMetadata(getUUID(ack.getMetadata().getSyncRequestId()), ack.getMetadata().getTimestamp(), ack.getMetadata().getSnapshotTimestamp())));

        blockUntilFSMTransition.countDown();
    }

    @FunctionalInterface
    public static interface TransitionSource {
        void changeState();
    }

    public enum WAIT {
        ON_ACK,
        ON_ACK_TS,
        ON_ERROR,
        ON_TIMEOUT_ERROR,
        ON_RESCHEDULE_SNAPSHOT_SYNC,
        ON_SINK_RECEIVE,
        ON_METADATA_RESPONSE,
        NONE
    }

    @Data
    public static class TestConfig {
        private int dropMessageLevel = 0;
        /**
         * 0 : No ACKs dropped
         * 1 : Arbitrarily ACKs are dropped
         * 2 : An ACK dropped and further messages dropped at Source.
         * */
        private int dropAckLevel = 0;
        private int delayedApplyCycles = 0; // Represents the number of cycles for which snapshot sync apply queries
                                            // reply that it has still not completed.
        private boolean trim = false;
        private boolean writingSrc = false;
        private boolean writingDst = false;
        private boolean deleteOP = false;
        private WAIT waitOn = WAIT.ON_ACK;
        private boolean timeoutMetadataResponse = false;
        private String remoteClusterId = null;

        public TestConfig clear() {
            dropMessageLevel = 0;
            dropAckLevel = 0;
            delayedApplyCycles = 0;
            timeoutMetadataResponse = false;
            trim = false;
            writingSrc = false;
            writingDst = false;
            deleteOP = false;
            remoteClusterId = null;
            return this;
        }
    }
}
