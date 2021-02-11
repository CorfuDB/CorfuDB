package org.corfudb.integration;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.integration.ReplicationReaderWriterIT.ckStreamsAndTrim;

import com.google.common.reflect.TypeToken;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationStateType;
import org.corfudb.infrastructure.logreplication.replication.fsm.ObservableAckMsg;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

/**
 * Test the core components of log replication, namely, Snapshot Sync and Log Entry Sync,
 * i.e., the ability to transfer a full view (snapshot) or incremental view of the datastore
 * from a source to a destination. In these tests we disregard communication channels between
 * clusters (sites) or CorfuLogReplicationServer.
 *
 * We emulate the channel by implementing a test data plane which directly forwards the data
 * to the SinkManager. Overall, these tests bring up two CorfuServers (datastore components),
 * one performing as the active and the other as the standby. We write different patterns of data
 * on the source (transactional and non transactional, as well as polluted and non-polluted transactions, i.e.,
 * transactions containing federated and non-federated streams) and verify that complete data
 * reaches the destination after initiating log replication.
 */
@Slf4j
public class LogReplicationIT extends AbstractIT implements Observer {

    public final static String nettyConfig = "src/test/resources/transport/nettyConfig.properties";

    private static final String SOURCE_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    private static final int WRITER_PORT = DEFAULT_PORT + 1;
    private static final String DESTINATION_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;

    private static final String ACTIVE_CLUSTER_ID = UUID.randomUUID().toString();
    private static final String REMOTE_CLUSTER_ID = UUID.randomUUID().toString();
    private static final int CORFU_PORT = 9000;
    private static final String TABLE_PREFIX = "test";

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

    static private final int SMALL_MSG_SIZE = 200;

    static private TestConfig testConfig = new TestConfig();

    private Process sourceServer;
    private Process destinationServer;

    // Connect with sourceServer to generate data
    private CorfuRuntime srcDataRuntime = null;

    // Connect with sourceServer to read snapshot data
    private CorfuRuntime readerRuntime = null;

    // Connect with destinationServer to write snapshot data
    private CorfuRuntime writerRuntime = null;

    // Connect with destinationServer to verify data
    private CorfuRuntime dstDataRuntime = null;

    private SourceForwardingDataSender sourceDataSender;

    // List of all opened maps backed by Corfu on Source and Destination
    private HashMap<String, CorfuTable<Long, Long>> srcCorfuTables = new HashMap<>();
    private HashMap<String, CorfuTable<Long, Long>> dstCorfuTables = new HashMap<>();

    // The in-memory data for corfu tables for verification.
    private HashMap<String, HashMap<Long, Long>> srcDataForVerification = new HashMap<>();
    private HashMap<String, HashMap<Long, Long>> dstDataForVerification = new HashMap<>();

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
    private long expectedAckTimestamp = Long.MAX_VALUE;

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

    private final String t0 = TABLE_PREFIX + 0;
    private final String t1 = TABLE_PREFIX + 1;
    private final String t2 = TABLE_PREFIX + 2;

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
        sourceServer = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        // Destination Corfu Server (data will be replicated into this server)
        destinationServer = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        srcDataRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        srcDataRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        srcDataRuntime.connect();

        srcTestRuntime = CorfuRuntime.fromParameters(params);
        srcTestRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        srcTestRuntime.connect();

        readerRuntime = CorfuRuntime.fromParameters(params);
        readerRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        readerRuntime.connect();

        writerRuntime = CorfuRuntime.fromParameters(params);
        writerRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        writerRuntime.connect();

        dstDataRuntime = CorfuRuntime.fromParameters(params);
        dstDataRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        dstDataRuntime.connect();

        dstTestRuntime = CorfuRuntime.fromParameters(params);
        dstTestRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        dstTestRuntime.connect();

        logReplicationMetadataManager = new LogReplicationMetadataManager(dstTestRuntime, 0, ACTIVE_CLUSTER_ID);
        testConfig.clear();
    }

    private void cleanEnv() {
        if (srcDataRuntime != null) {
            srcDataRuntime.shutdown();
            srcTestRuntime.shutdown();
            dstDataRuntime.shutdown();
            dstTestRuntime.shutdown();

            readerRuntime.shutdown();
            writerRuntime.shutdown();
        }

        if (sourceDataSender != null) {
            sourceDataSender.shutdown();
        }
    }

    /**
     * Open numStreams with fixed names 'TABLE_PREFIX' index and add them to 'tables'
     *
     * @param tables map to populate with opened streams
     * @param rt corfu runtime
     * @param numStreams number of streams to open
     */
    private void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt, int numStreams) {
        for (int i = 0; i < numStreams; i++) {
            String name = TABLE_PREFIX + i;

            CorfuTable<Long, Long> table = rt.getObjectsView()
                    .build()
                    .setStreamName(name)
                    .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                    })
                    .setSerializer(Serializers.PRIMITIVE)
                    .open();
            tables.put(name, table);
        }
    }

    /**
     * Generate Transactional data on 'tables' and push the data for
     * further verification into an in-memory copy 'tablesForVerification'.
     */
    private void generateTXData(HashMap<String, CorfuTable<Long, Long>> tables,
                      HashMap<String, HashMap<Long, Long>> hashMap,
                      int numKeys, CorfuRuntime rt, long startValue) {
        for (String streamName : tables.keySet()) {
            generateTransactionsCrossTables(tables, Collections.singleton(streamName), hashMap, numKeys, rt, startValue);
        }
    }

    /**
     * Generate Transactional data across several 'tablesCrossTxs' and push the data for
     * further verification into an in-memory copy 'tablesForVerification'.
     */
    private void generateTransactionsCrossTables(HashMap<String, CorfuTable<Long, Long>> tables,
                                                 Set<String> tablesCrossTxs,
                                                 HashMap<String, HashMap<Long, Long>> tablesForVerification,
                                                 int numKeys, CorfuRuntime rt, long startValue) {
        int cntDelete = 0;
        for (int i = 0; i < numKeys; i++) {
            rt.getObjectsView().TXBegin();
            for (String name : tablesCrossTxs) {
                tablesForVerification.putIfAbsent(name, new HashMap<>());
                long key = i + startValue;
                tables.get(name).put(key, key);
                tablesForVerification.get(name).put(key, key);

                // delete keys randomly
                if (testConfig.deleteOP && (i % DELETE_PACE == 0)) {
                    tables.get(name).delete(key - 1);
                    tablesForVerification.get(name).remove(key - 1);
                    cntDelete++;
                }
            }
            rt.getObjectsView().TXEnd();
            long tail = Utils.getLogAddressSpace(rt
                    .getLayoutView().getRuntimeLayout())
                    .getAddressMap()
                    .get(ObjectsView.TRANSACTION_STREAM_ID).getTail();
            expectedAckTimestamp = Math.max(tail, expectedAckTimestamp);
        }

        if (cntDelete > 0) {
            log.debug("delete cnt {}", cntDelete);
        }
    }


    private void verifyTables(HashMap<String, CorfuTable<Long, Long>> tables0, HashMap<String, CorfuTable<Long, Long>> tables1) {
            for (String name : tables0.keySet()) {
                CorfuTable<Long, Long> table = tables0.get(name);
                CorfuTable<Long, Long> mapKeys = tables1.get(name);

                //System.out.print("\nTable[" + name + "]: " + table.keySet().size() + " keys; Expected "
                //        + mapKeys.size() + " keys");

                assertThat(mapKeys.keySet().containsAll(table.keySet())).isTrue();
                assertThat(table.keySet().containsAll(mapKeys.keySet())).isTrue();
                assertThat(table.keySet().size() == mapKeys.keySet().size()).isTrue();

                for (Long key : mapKeys.keySet()) {
                    assertThat(table.get(key)).isEqualTo(mapKeys.get(key));
                }
            }
    }

    /**
     * Wait replication data reach at the standby cluster.
     * @param tables
     * @param hashMap
     */
    private void waitData(HashMap<String, CorfuTable<Long, Long>> tables, HashMap<String, HashMap<Long, Long>> hashMap) {
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            HashMap<Long, Long> mapKeys = hashMap.get(name);
            while (table.size() < mapKeys.size()) {
                //
            }
        }
    }

    private void verifyData(HashMap<String, CorfuTable<Long, Long>> tables, HashMap<String, HashMap<Long, Long>> hashMap) {
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            HashMap<Long, Long> mapKeys = hashMap.get(name);

            log.debug("Table[" + name + "]: " + table.keySet().size() + " keys; Expected "
                    + mapKeys.size() + " keys");

            assertThat(mapKeys.keySet().containsAll(table.keySet())).isTrue();
            assertThat(table.keySet().containsAll(mapKeys.keySet())).isTrue();
            assertThat(table.keySet().size() == mapKeys.keySet().size()).isTrue();

            for (Long key : mapKeys.keySet()) {
                assertThat(table.get(key)).isEqualTo(mapKeys.get(key));
            }
        }
    }

    private void verifyNoData(HashMap<String, CorfuTable<Long, Long>> tables) {
        for (CorfuTable table : tables.values()) {
            assertThat(table.keySet().isEmpty());
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
        testSnapshotSyncAndLogEntrySync(0, false);
    }

    /**
     * In this test we emulate the following scenario, 3 tables (T0, T1, T2). Only T0 and T1 are replicated.
     * We write following this pattern:
     *
     * - Write transactions across T0 and T1.
     * - Write individual transactions for T0 and T1
     * - Write transactions to T2 but do not replicate.
     *
     *
     * @throws Exception
     */
    @Test
    public void testValidSnapshotSyncCrossTables() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        writeCrossTableTransactions(crossTables, true);

        // Start Snapshot Sync
        startSnapshotSync(crossTables);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");

        // Because t2 should not have been replicated remove from expected list
        srcDataForVerification.get(t2).clear();

        verifyData(dstCorfuTables, srcDataForVerification);

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
        crossTables.add(t0);
        crossTables.add(t1);
        crossTables.add(t2);

        // Replicate Tables
        Set<String> replicateTables = new HashSet<>();
        replicateTables.add(t0);
        replicateTables.add(t1);

        writeCrossTableTransactions(crossTables, true);

        // Start Snapshot Sync
        startSnapshotSync(replicateTables);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");
        // Because t2 should not have been replicated remove from expected list
        srcDataForVerification.get(t2).clear();
        verifyData(dstCorfuTables, srcDataForVerification);
        cleanEnv();
    }

    /**
     * In this test we check the case where no tables were specified to be replicated,
     * here we would expect the snapshot sync to complete with no actual data showing up
     * on the destination.
     *
     * @throws Exception
     */
    @Test
    public void testSnapshotSyncNoTablesToReplicate() throws Exception {
        // Write data in transaction to t0, t1 and t2 (where t2 is not intended to be replicated)
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);
        crossTables.add(t2);

        writeCrossTableTransactions(crossTables, true);

        // Start Snapshot Sync, indicating an empty set of tables to replicate. This is not allowed
        // and we should expect an exception.
        assertThatThrownBy(() -> startSnapshotSync(new HashSet<>())).isInstanceOf(IllegalArgumentException.class);
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
        openStreams(srcCorfuTables, srcDataRuntime, NUM_STREAMS);

        // Verify no data on source
        log.debug("****** Verify No Data in Source Site");
        verifyNoData(srcCorfuTables);

        // Verify destination tables have no actual data.
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        log.debug("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);

        // Tables to Replicate
        Set<String> tablesToReplicate = new HashSet<>();
        tablesToReplicate.add(t0);
        tablesToReplicate.add(t1);

        // We don't write data to the log
        // StartSnapshotSync (no actual data present in the log)
        startSnapshotSync(tablesToReplicate);

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
        openStreams(srcCorfuTables, srcDataRuntime, TOTAL_STREAM_COUNT);
        Set<String> tablesAcrossTx = new HashSet<>(Arrays.asList(t0, t1, t2));
        generateTransactionsCrossTables(srcCorfuTables, tablesAcrossTx, srcDataForVerification, NUM_KEYS*NUM_KEYS, srcDataRuntime, 0);

        // Verify data on source is actually present
        log.debug("****** Verify Data in Source Site");
        verifyData(srcCorfuTables, srcDataForVerification);

        // Verify destination tables have no actual data before log replication
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        log.debug("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);

        // Tables to Replicate: non existing table
        String t3 = TABLE_PREFIX + TOTAL_STREAM_COUNT;
        Set<String> tablesToReplicate = new HashSet<>(Arrays.asList(t3));

        // We don't write data to the log
        // StartSnapshotSync (no actual data present in the log)
        startSnapshotSync(tablesToReplicate);

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
        openStreams(srcCorfuTables, srcDataRuntime, NUM_STREAMS);

        // Verify no data on source
        log.debug("****** Verify No Data in Source Site");
        verifyNoData(srcCorfuTables);

        // Verify destination tables have no actual data.
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        log.debug("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);

        // We did not write data to the log
        testConfig.clear();
        LogReplicationFSM fsm = startLogEntrySync(Collections.singleton(t0), WAIT.NONE);

        // Wait until Log Entry Starts
        checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);

        int retries = 0;
        // Check for several iterations that it is still in Log Entry Sync State and no Error because of empty log
        // shutdown the state machine
        while (retries < STATE_CHANGE_CHECKS) {
            checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, false);
            retries++;
            sleep(WAIT_STATE_CHANGE);
        }

        // Verify No Data On Destination
        verifyNoData(dstCorfuTables);

        cleanEnv();
    }

    /**
     * Test Log Entry Sync, when transactions are performed across valid tables
     * (i.e., all tables are set to be replicated).
     *
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncValidCrossTables() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        // Writes transactions to t0, t1 and t2 + transactions across 'crossTables'
        writeCrossTableTransactions(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages = NUM_KEYS * WRITE_CYCLES;
        testConfig.clear();

        startLogEntrySync(crossTables);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");
        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Destination
        verifyData(dstCorfuTables, srcDataForVerification);
    }


    /**
     * Test Log Entry (delta) Sync for the case where messages are dropped at the destination
     * for a fixed number of times. This will test messages being resent and further applied.
     */
    @Test
    public void testLogEntrySyncValidCrossTablesWithDropMsg() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        writeCrossTableTransactions(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;

        testConfig.clear().setDropMessageLevel(1);
        startLogEntrySync(crossTables, WAIT.ON_ACK);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");

        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Destination
        verifyData(dstCorfuTables, srcDataForVerification);
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
        crossTables.add(t0);
        crossTables.add(t1);
        crossTables.add(t2);

        // Writes transactions to t0, t1 and t2 + transactions across 'crossTables'
        writeCrossTableTransactions(crossTables, startWithCrossTableTxs);

        Set<String> replicateTables = new HashSet<>();
        replicateTables.add(t0);
        replicateTables.add(t1);

        // Start Log Entry Sync
        // We need to block until the error is received and verify the state machine is shutdown
        testConfig.clear();
        expectedAckMessages = Utils.getLogAddressSpace(srcDataRuntime
                .getLayoutView().getRuntimeLayout())
                .getAddressMap()
                .get(ObjectsView.TRANSACTION_STREAM_ID).getTail();

        LogReplicationFSM fsm = startLogEntrySync(replicateTables, WAIT.ON_ACK);

        checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");

        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Destination
        verifyData(dstCorfuTables, srcDataForVerification);
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
        crossTables.add(t0);
        crossTables.add(t1);

        writeCrossTableTransactions(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;

        testConfig.clear();
        testConfig.setWritingSrc(true);
        testConfig.setDeleteOP(true);
        testConfig.setWaitOn(WAIT.ON_ACK);

        HashSet<WAIT> waitHashSet = new HashSet<>();
        waitHashSet.add(WAIT.ON_ACK);
        startLogEntrySync(crossTables, waitHashSet, false);

        expectedAckTimestamp = Long.MAX_VALUE;

        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Data on Destination site
        log.debug("****** Wait Data on Destination");
        waitData(dstCorfuTables, srcDataForVerification);

        log.debug("****** Verify Data on Destination");
        // Verify Destination
        verifyData(dstCorfuTables, srcDataForVerification);
        expectedAckTimestamp = srcDataRuntime.getAddressSpaceView().getLogTail();
        assertThat(expectedAckTimestamp).isEqualTo(logReplicationMetadataManager.getLastProcessedLogEntryTimestamp());
        verifyPersistedSnapshotMetadata();
        verifyPersistedLogEntryMetadata();

        cleanEnv();
    }


    /**
     * This test attempts to perform a snapshot sync, when part of the log has already been trimmed before starting.
     * We expect the state machine to capture this event and move to the REQUIRE_SNAPSHOT_SYNC where it will
     * wait until snapshot sync is re-triggered.
     */
    @Test
    public void testSnapshotSyncWithInitialTrimmedExceptions() throws Exception {
        final int FRACTION_LOG_TRIM = 10;

        // Setup Environment: two corfu servers (source & destination)
        setupEnv();

        // Open One Stream
        openStreams(srcCorfuTables, srcDataRuntime, 1);
        openStreams(dstCorfuTables, dstDataRuntime, 1);

        // Let's generate data on Source Corfu Server to be Replicated
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS_LARGE, srcDataRuntime, 0);

        // Trim part of the log before starting
        trim(srcDataRuntime, NUM_KEYS_LARGE/FRACTION_LOG_TRIM);

        // Replicate the only table we created
        // Wait until an error occurs and drop snapshot sync request or we'll get into an infinite loop
        // as the snapshot sync will always fail due to the log being trimmed with no checkpoint.
        expectedErrors = 1;
        startSnapshotSync(srcCorfuTables.keySet(), WAIT.ON_ERROR);

        // Verify its in require snapshot sync state and that data was not completely transferred to destination
        // checkStateChange(sourceManager.getLogReplicationFSM(), LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC, true);
        verifyNoData(dstCorfuTables);
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
        openStreams(srcCorfuTables, srcDataRuntime, 1);
        openStreams(dstCorfuTables, dstDataRuntime, 1);

        // Let's generate data on Source Corfu Server to be Replicated
        // Write a very large number of entries, so we can be sure the trim happens during snapshot sync
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS_LARGE, srcDataRuntime, 0);

        // Replicate the only table we created, block until 2 messages are received,
        // then enforce a trim on the log.
        expectedSinkReceivedMessages = RX_MESSAGES_LIMIT;
        expectedAckTimestamp = -1;
        testConfig.setWaitOn(WAIT.ON_ACK_TS);

        LogReplicationSourceManager sourceManager = startSnapshotSync(srcCorfuTables.keySet(),
                new HashSet<>(Arrays.asList(WAIT.ON_ACK, WAIT.ON_ACK_TS, WAIT.ON_ERROR, WAIT.ON_SINK_RECEIVE)));

        // KWrite a checkpoint and trim
        Token token = ckStreamsAndTrim(srcDataRuntime, srcCorfuTables);
        srcDataRuntime.getAddressSpaceView().invalidateServerCaches();
        expectedAckTimestamp = srcDataRuntime.getAddressSpaceView().getLogTail();

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
     * This test attempts to perform a Log Entry sync, when part of the log has already been trimmed before starting.
     *
     * We expect the state machine to capture this event and move to the REQUIRE_SNAPSHOT_SYNC where it will
     * wait until snapshot sync is triggered.
     */
    @Test
    public void testLogEntrySyncWithInitialTrimmedExceptions() throws Exception {
        final int FRACTION_LOG_TRIM = 10;

        // Setup Environment: two corfu servers (source & destination)
        setupEnv();

        // Open One Stream
        openStreams(srcCorfuTables, srcDataRuntime, 1);
        openStreams(dstCorfuTables, dstDataRuntime, 1);

        // Let's generate data on Source Corfu Server to be Replicated
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS_LARGE, srcDataRuntime, 0);

        // Trim part of the log before starting
        trim(srcDataRuntime, NUM_KEYS_LARGE/FRACTION_LOG_TRIM);

        // Replicate the only table we created
        // Wait until an error occurs and drop snapshot sync request or we'll get into an infinite loop
        // as the snapshot sync will always fail due to the log being trimmed with no checkpoint.
        expectedErrors = 1;
        startLogEntrySync(srcCorfuTables.keySet(), WAIT.ON_ERROR, true);

        // Verify its in require snapshot sync state and that data was not completely transferred to destination
//        checkStateChange(fsm, LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC, true);
        log.debug("****** Verify no data at destination");
        verifyNoData(dstCorfuTables);
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
        openStreams(srcCorfuTables, srcDataRuntime, 1);
        openStreams(dstCorfuTables, dstDataRuntime, 1);

        // Let's generate data on Source Corfu Server to be Replicated
        // Write a large number of entries, so we can be sure the trim happens during log entry sync
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS_LARGE, srcDataRuntime, 0);

        // Replicate the only table we created, block until 2 messages are received,
        // then enforce a trim on the log.
        expectedSinkReceivedMessages = RX_MESSAGES_LIMIT;

        LogReplicationFSM fsm = startLogEntrySync(srcCorfuTables.keySet(), WAIT.ON_SINK_RECEIVE, false);

        log.debug("****** Trim log, will trigger a snapshot sync");

        Token toke = ckStreamsAndTrim(srcDataRuntime, srcCorfuTables);

        // Be sure log was trimmed
        while (srcDataRuntime.getAddressSpaceView().getTrimMark().getSequence() < toke.getSequence()) {
            // no-op
        }

        log.debug("****** Wait until IN_SNAPSHOT_SYNC");
        checkStateChange(fsm, LogReplicationStateType.IN_SNAPSHOT_SYNC, true);

        log.debug("Block until full snapshot transfer complete");
        checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);

        verifyData(dstCorfuTables, dstDataForVerification);
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
        openStreams(srcCorfuTables, srcDataRuntime, 1);
        openStreams(dstCorfuTables, dstDataRuntime, 1);

        int num_keys = NUM_KEYS_VERY_LARGE;

        // Let's generate data on Source Corfu Server to be Replicated
        // Write a very large number of entries, so we can be sure the trim happens during snapshot sync
        log.debug("****** Generate TX Data");
        generateTXData(srcCorfuTables, srcDataForVerification, num_keys, srcDataRuntime, 0);

        // Replicate the only table we created, block until 10 messages are received,
        // then enforce a trim on the log.
        log.debug("****** Start Log Entry Sync");
        expectedAckMessages = num_keys;
        LogReplicationFSM fsm = startLogEntrySync(srcCorfuTables.keySet(), WAIT.ON_ACK, false);

        log.debug("****** Total " + num_keys + " ACK messages received. Verify State.");

        // Verify its in log entry sync state and that data was completely transferred to destination
        checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);
        verifyData(dstCorfuTables, dstDataForVerification);
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
        testSnapshotSyncAndLogEntrySync(numCyclesToDelayApply, false);
    }

    /**
     * Test the case where a Snapshot Sync apply metadata response is delayed causing TimeoutExceptions.
     * Verify we are able to recover.
     */
    @Test
    public void testSnapshotSyncDelayedApplyResponse() throws Exception {
        testSnapshotSyncAndLogEntrySync(0, true);
    }

    private void testSnapshotSyncAndLogEntrySync(int numCyclesToDelayApply, boolean delayResponse) throws Exception {
        // Setup two separate Corfu Servers: source (active) and destination (standby)
        setupEnv();

        // Open streams in source Corfu
        openStreams(srcCorfuTables, srcDataRuntime, NUM_STREAMS);

        // Write data into Source Tables
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // Verify data just written against in-memory copy
        log.debug("****** Verify Source Data");
        verifyData(srcCorfuTables, srcDataForVerification);

        // Before initiating log replication, verify these tables have no actual data in the destination.
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        log.debug("****** Verify No Data in Destination");
        verifyNoData(dstCorfuTables);

        expectedAckTimestamp = Long.MAX_VALUE;
        testConfig.setDelayedApplyCycles(numCyclesToDelayApply);
        testConfig.setTimeoutMetadataResponse(delayResponse);

        // Start Snapshot Sync (through Source Manager)
        Set<WAIT> conditions = new HashSet<>();
        conditions.add(WAIT.ON_METADATA_RESPONSE); // Condition to wait for snapshot sync
        conditions.add(WAIT.ON_ACK_TS); // Condition to wait for log entry sync
        startSnapshotSync(srcCorfuTables.keySet(), conditions);

        log.debug("****** Snapshot Sync COMPLETE");

        testConfig.setWaitOn(WAIT.ON_ACK_TS);

        // Verify Data on Destination site
        log.debug("****** Verify Data on Destination");
        verifyData(dstCorfuTables, srcDataForVerification);

        blockUntilExpectedAckTs.acquire();
        expectedAckTimestamp = srcDataRuntime.getAddressSpaceView().getLogTail() + NUM_KEYS;

        // Write Extra Data (for incremental / log entry sync)
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS_LARGE, srcDataRuntime, NUM_KEYS*2);

        // Block until the log entry sync completes == expected number of ACKs are received
        log.debug("****** Wait until log entry sync completes and ACKs are received");
        blockUntilExpectedAckTs.acquire();

        // Verify Data at Destination
        log.debug("****** Verify Destination Data for log entry (incremental updates)");
        verifyData(dstCorfuTables, srcDataForVerification);

        verifyPersistedSnapshotMetadata();
        verifyPersistedLogEntryMetadata();
    }

    /* ********************** AUXILIARY METHODS ********************** */

    // startCrossTx indicates if we start with a transaction across Tables
    private void writeCrossTableTransactions(Set<String> crossTableTransactions, boolean startCrossTx) throws Exception {
        // Setup two separate Corfu Servers: source (primary) and destination (standby)
        setupEnv();

        // Open streams in source Corfu
        int totalStreams = TOTAL_STREAM_COUNT; // test0, test1, test2 (open stream tables)
        openStreams(srcCorfuTables, srcDataRuntime, totalStreams);

        // Write data across to tables specified in crossTableTransactions in transaction
        if (startCrossTx) {
            generateTransactionsCrossTables(srcCorfuTables, crossTableTransactions, srcDataForVerification, NUM_KEYS, srcDataRuntime, 0);
        }

        // Write data to t0
        generateTransactionsCrossTables(srcCorfuTables, Collections.singleton(t0), srcDataForVerification, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // Write data to t1
        generateTransactionsCrossTables(srcCorfuTables, Collections.singleton(t1), srcDataForVerification, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // Write data to t2
        generateTransactionsCrossTables(srcCorfuTables, Collections.singleton(t2), srcDataForVerification, NUM_KEYS, srcDataRuntime, 0);

        // Write data across to tables specified in crossTableTransactions in transaction
        generateTransactionsCrossTables(srcCorfuTables, crossTableTransactions, srcDataForVerification, NUM_KEYS, srcDataRuntime, NUM_KEYS*2);

        // Verify data just written against in-memory copy
        verifyData(srcCorfuTables, srcDataForVerification);

        // Before initiating log replication, verify these tables have no actual data in the destination node.
        openStreams(dstCorfuTables, dstDataRuntime, totalStreams);
        log.debug("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);
    }

    private void startTxAtSrc() {
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        generateTransactionsCrossTables(srcCorfuTables, crossTables, srcDataForVerification,
                NUM_KEYS_LARGE, srcDataRuntime, NUM_KEYS*WRITE_CYCLES);
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

    private LogReplicationSourceManager startSnapshotSync(Set<String> tablesToReplicate) throws Exception {
        return startSnapshotSync(tablesToReplicate, WAIT.ON_METADATA_RESPONSE);
    }

    private LogReplicationSourceManager startSnapshotSync(Set<String> tablesToReplicate, WAIT waitCondition) throws Exception {
        return startSnapshotSync(tablesToReplicate, new HashSet<>(Arrays.asList(waitCondition)));
    }

    private LogReplicationSourceManager startSnapshotSync(Set<String> tablesToReplicate, Set<WAIT> waitConditions) throws Exception {

        // Observe metadata responses coming from receiver, until it indicates snapshot sync apply has completed
        blockUntilExpectedMetadataResponse.acquire();

        LogReplicationSourceManager logReplicationSourceManager = setupSourceManagerAndObservedValues(tablesToReplicate,
                waitConditions);

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

    private LogReplicationFSM startLogEntrySync(Set<String> tablesToReplicate) throws Exception {
        testConfig.setWaitOn(WAIT.ON_ACK);
        return startLogEntrySync(tablesToReplicate, WAIT.ON_ACK);
    }

    /**
     * Start Log Entry Sync
     *
     * @param tablesToReplicate set of tables to replicate
     * @param waitCondition an enum indicating if we should wait for an ack. If false, we should wait for an error
     * @throws Exception
     */
    private LogReplicationFSM startLogEntrySync(Set<String> tablesToReplicate, WAIT waitCondition) throws Exception {
        return startLogEntrySync(tablesToReplicate, waitCondition, true);
    }

    private LogReplicationFSM startLogEntrySync(Set<String> tablesToReplicate, WAIT waitCondition,
                                                boolean injectTxData) throws Exception {
        HashSet<WAIT> conditions = new HashSet<>();
        conditions.add(waitCondition);
        return startLogEntrySync(tablesToReplicate, conditions, injectTxData);
    }

    private LogReplicationFSM startLogEntrySync(Set<String> tablesToReplicate, Set<WAIT> waitConditions,
                                                boolean injectTxData) throws Exception {

        LogReplicationSourceManager logReplicationSourceManager = setupSourceManagerAndObservedValues(tablesToReplicate,
                waitConditions);

        // Start Log Entry Sync
        log.debug("****** Start Log Entry Sync with src tail " + srcDataRuntime.getAddressSpaceView().getLogTail()
                + " dst tail " + dstDataRuntime.getAddressSpaceView().getLogTail());
        logReplicationSourceManager.startReplication(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST,
                new LogReplicationEventMetadata(UUID.randomUUID(), -1, -1)));

        // Start TX's in parallel, while log entry sync is running
        if (injectTxData) {
            startTx();
        }

        blockUntilExpectedAckTs.acquire();
        expectedAckTimestamp = srcDataRuntime.getAddressSpaceView().getLogTail();

        // Block until the expected ACK Timestamp is reached
        log.debug("****** Wait until the wait condition is met");
        if (waitConditions.contains(WAIT.ON_ERROR) || waitConditions.contains(WAIT.ON_TIMEOUT_ERROR)) {
            blockUntilExpectedValueReached.acquire();
        } else if (waitConditions.contains(WAIT.ON_ACK)) {
            blockUntilExpectedAckTs.acquire();
        }

        return logReplicationSourceManager.getLogReplicationFSM();
    }

    private LogReplicationSourceManager setupSourceManagerAndObservedValues(Set<String> tablesToReplicate,
                                                                            Set<WAIT> waitConditions) throws InterruptedException {

        LogReplicationConfig config = new LogReplicationConfig(tablesToReplicate, BATCH_SIZE, SMALL_MSG_SIZE);

        // Data Sender
        sourceDataSender = new SourceForwardingDataSender(DESTINATION_ENDPOINT, config, testConfig,
                logReplicationMetadataManager, nettyConfig);

        // Source Manager
        LogReplicationSourceManager logReplicationSourceManager = new LogReplicationSourceManager(
                LogReplicationRuntimeParameters.builder()
                        .remoteClusterDescriptor(new ClusterDescriptor(REMOTE_CLUSTER_ID,
                                LogReplicationClusterInfo.ClusterRole.ACTIVE, CORFU_PORT))
                                .replicationConfig(config).localCorfuEndpoint(SOURCE_ENDPOINT).build(),
                logReplicationMetadataManager,
                sourceDataSender);
        logReplicationSourceManager.getLogReplicationFSM().getAckReader().getOngoing().set(false);

        // Set Log Replication Source Manager so we can emulate the channel for data & control messages (required
        // for testing)
        sourceDataSender.setSourceManager(logReplicationSourceManager);

        // Add this class as observer of the value of interest for the wait condition
        for (WAIT waitCondition : waitConditions) {
            switch(waitCondition) {
                case ON_ACK:
                case ON_ACK_TS:
                    ackMessages = sourceDataSender.getAckMessages();
                    ackMessages.addObserver(this);
                    break;
                case ON_ERROR: // Wait on Error Notifications to Source
                case ON_TIMEOUT_ERROR:
                    errorsLogEntrySync = sourceDataSender.getErrors();
                    errorsLogEntrySync.addObserver(this);
                    break;
                case ON_METADATA_RESPONSE:
                    metadataResponseObservable = sourceDataSender.getMetadataResponses();
                    metadataResponseObservable.addObserver(this);
                case ON_SINK_RECEIVE: // Wait on Received Messages on Sink (Destination)
                    sinkReceivedMessages = sourceDataSender.getSinkManager().getRxMessageCount();
                    sinkReceivedMessages.addObserver(this);
                default:
                    // Nothing
                    break;
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
            log.debug("Metadata response indicates snapshot sync apply is still in progress");
        }
    }

    private void verifyExpectedValue(long expectedValue, long currentValue) {
        // If expected value, release semaphore / unblock the wait
        if (expectedValue == currentValue) {
            if (expectedAckMsgType != null) {
                blockUntilExpectedValueReached.release();
            }
        }
    }

    private void verifyExpectedAckMessage(ObservableAckMsg observableAckMsg) {
        // If expected a ackTs, release semaphore / unblock the wait
        if (observableAckMsg.getDataMessage() != null) {
            LogReplicationEntryMsg logReplicationEntry = observableAckMsg.getDataMessage();

            switch (testConfig.waitOn) {
                case ON_ACK:
                    verifyExpectedValue(expectedAckMessages, ackMessages.getMsgCnt());
                case ON_ACK_TS:
                    verifyExpectedValue(expectedAckTimestamp, logReplicationEntry.getMetadata().getTimestamp());
                    if (expectedAckMsgType == logReplicationEntry.getMetadata().getEntryType()) {
                        blockUntilExpectedAckType.release();
                    }

                    log.debug("expectedAckTs={}, logEntryTs={}", expectedAckTimestamp, logReplicationEntry.getMetadata().getTimestamp());

                    if (expectedAckTimestamp == logReplicationEntry.getMetadata().getTimestamp()) {
                        blockUntilExpectedAckTs.release();
                    }
                    break;
                default:
                    break;
            }
        }
    }

    private void verifyPersistedSnapshotMetadata() {
        long lastSnapStart = logReplicationMetadataManager.getLastStartedSnapshotTimestamp();
        long lastSnapDone = logReplicationMetadataManager.getLastAppliedSnapshotTimestamp();

        log.debug("\nlastSnapStart " + lastSnapStart + " lastSnapDone " + lastSnapDone);
        assertThat(lastSnapStart == lastSnapDone).isTrue();
    }

    private void verifyPersistedLogEntryMetadata() {
        long lastLogProcessed = logReplicationMetadataManager.getLastProcessedLogEntryTimestamp();

        log.debug("\nlastLogProcessed " + lastLogProcessed + " expectedTimestamp " + expectedAckTimestamp);
        assertThat(expectedAckTimestamp == lastLogProcessed).isTrue();
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
        private int delayedApplyCycles = 0; // Represents the number of cycles for which snapshot sync apply queries
                                            // reply that it has still not completed.
        private boolean trim = false;
        private boolean writingSrc = false;
        private boolean writingDst = false;
        private boolean deleteOP = false;
        private WAIT waitOn = WAIT.ON_ACK;
        private boolean timeoutMetadataResponse = false;
        
        public TestConfig clear() {
            dropMessageLevel = 0;
            delayedApplyCycles = 0;
            timeoutMetadataResponse = false;
            trim = false;
            writingSrc = false;
            writingDst = false;
            deleteOP = false;
            return this;
        }
    }
}
