package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationStateType;
import org.corfudb.infrastructure.logreplication.replication.fsm.ObservableAckMsg;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

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

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.corfudb.integration.ReplicationReaderWriterIT.ckStreamsAndTrim;

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

    static final String SOURCE_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    static final int WRITER_PORT = DEFAULT_PORT + 1;
    static final String DESTINATION_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;

    static final String ACTIVE_CLUSTER_ID = UUID.randomUUID().toString();
    static final String REMOTE_CLUSTER_ID = UUID.randomUUID().toString();
    static final String TABLE_PREFIX = "test";

    static private final int NUM_KEYS = 4;

    static private final int NUM_KEYS_LARGE = 1000;
    static private final int NUM_KEYS_VERY_LARGE = 20000;

    static private final int NUM_STREAMS = 1;
    static private final int TOTAL_STREAM_COUNT = 3;
    static private final int WRITE_CYCLES = 4;

    static private final int STATE_CHANGE_CHECKS = 20;
    static private final int WAIT_STATE_CHANGE = 300;

    // If testConfig set deleteOp enabled, will have one delete operation for four put operations.
    static private final int DELETE_PACE = 4;

    static private TestConfig testConfig = new TestConfig();

    Process sourceServer;
    Process destinationServer;

    // Connect with sourceServer to generate data
    CorfuRuntime srcDataRuntime = null;

    // Connect with sourceServer to read snapshot data
    CorfuRuntime readerRuntime = null;

    // Connect with destinationServer to write snapshot data
    CorfuRuntime writerRuntime = null;

    // Connect with destinationServer to verify data
    CorfuRuntime dstDataRuntime = null;

    SourceForwardingDataSender sourceDataSender;

    // List of all opened maps backed by Corfu on Source and Destination
    HashMap<String, CorfuTable<Long, Long>> srcCorfuTables = new HashMap<>();
    HashMap<String, CorfuTable<Long, Long>> dstCorfuTables = new HashMap<>();

    // The in-memory data for corfu tables for verification.
    HashMap<String, HashMap<Long, Long>> srcDataForVerification = new HashMap<>();
    HashMap<String, HashMap<Long, Long>> dstDataForVerification = new HashMap<>();

    CorfuRuntime srcTestRuntime;

    CorfuRuntime dstTestRuntime;

    /* ********* Test Observables ********** */

    // An observable value on the number of received ACKs (source side)
    private ObservableAckMsg ackMessages;

    // An observable value on the number of errors received on Log Entry Sync (source side)
    private ObservableValue errorsLogEntrySync;

    // An observable value oln the number of received messages in the LogReplicationSinkManager (Destination Site)
    private ObservableValue sinkReceivedMessages;

    /* ******** Expected Values on Observables ******** */

    // Set per test according to the expected number of ACKs that will unblock the code waiting for the value change
    private int expectedAckMessages = 0;

    // Set per test according to the expected ACK's timestamp.
    private long expectedAckTimestamp = Long.MAX_VALUE;

    // Set per test according to the expected number of errors in a test
    private int expectedErrors = 1;

    // Set per test according to the expected number ti
    private int expectedSinkReceivedMessages = 0;

    private MessageType expectedAckMsgType = MessageType.SNAPSHOT_END;

    /* ********* Semaphore to block until expected values are reached ********** */

    // A semaphore that allows to block until the observed value reaches the expected value
    private final Semaphore blockUntilExpectedValueReached = new Semaphore(1, true);


    // A semaphore that allows to block until the observed value reaches the expected value
    private final Semaphore blockUntilExpectedAckType = new Semaphore(1, true);

    private final Semaphore blockUntilExpectedAckTs = new Semaphore(1, true);

    private LogReplicationMetadataManager logReplicationMetadataManager;

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
            long tail = rt.getAddressSpaceView().getLogAddressSpace().getAddressMap().get(ObjectsView.TRANSACTION_STREAM_ID).getTail();
            expectedAckTimestamp = Math.max(tail, expectedAckTimestamp);
        }

        if (cntDelete > 0) {
            System.out.println("\ndelete cnt " + cntDelete);
        }
    }


    void verifyTables(HashMap<String, CorfuTable<Long, Long>> tables0, HashMap<String, CorfuTable<Long, Long>> tables1) {
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

    void verifyData(HashMap<String, CorfuTable<Long, Long>> tables, HashMap<String, HashMap<Long, Long>> hashMap) {
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            HashMap<Long, Long> mapKeys = hashMap.get(name);

            System.out.println("Table[" + name + "]: " + table.keySet().size() + " keys; Expected "
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

        // Setup two separate Corfu Servers: source (primary) and destination (standby)
        setupEnv();

        // Open streams in source Corfu
        openStreams(srcCorfuTables, srcDataRuntime, NUM_STREAMS);

        // Write data into Source Tables
        //generateData(srcCorfuTables, srcDataForVerification, NUM_KEYS, NUM_KEYS);
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // Verify data just written against in-memory copy
        System.out.println("\n****** Verify Source Data");
        verifyData(srcCorfuTables, srcDataForVerification);

        // Before initiating log replication, verify these tables have no actual data in the destination node.
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        System.out.println("\n****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);
        expectedAckTimestamp = Long.MAX_VALUE;
        // Start Snapshot Sync (through Source Manager)
        startSnapshotSync(srcCorfuTables.keySet());

        System.out.println("\n>>>>> END Snapshot Sync");

        // Verify Data on Destination site
        System.out.println("\n****** Verify Data on Destination");
        verifyData(dstCorfuTables, srcDataForVerification);

        // Reset expected messages for number of ACKs expected (log entry sync batches + 1 from snapshot sync)
        expectedAckMessages = (NUM_KEYS) + 1;

        blockUntilExpectedAckTs.acquire();
        expectedAckTimestamp = srcDataRuntime.getAddressSpaceView().getLogTail() + NUM_KEYS;
        // Write Extra Data (for incremental / log entry sync)
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS, srcDataRuntime, NUM_KEYS*2);
        System.out.println("\n****** Verify Source Data for log entry (incremental updates) till tail " + expectedAckTimestamp);
        verifyData(srcCorfuTables, srcDataForVerification);

        System.out.println("\n***** Start Log Entry Replication");

        // Block until the log entry sync completes == expected number of ACKs are received
        System.out.println("\n****** Wait until log entry sync completes and ACKs are received");
        blockUntilExpectedAckTs.acquire();

        // Verify Data at Destination
        System.out.println("\n****** Verify Destination Data for log entry (incremental updates)");
        verifyData(dstCorfuTables, srcDataForVerification);

        verifyPersistedSnapshotMetadata();
        verifyPersistedLogEntryMetadata();
    }

    private final String t0 = TABLE_PREFIX + 0;
    private final String t1 = TABLE_PREFIX + 1;
    private final String t2 = TABLE_PREFIX + 2;

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

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Snapshot Sync
        startSnapshotSync(crossTables);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 should not have been replicated remove from expected list
        srcDataForVerification.get(t2).clear();
        verifyData(dstCorfuTables, srcDataForVerification);

        cleanEnv();
    }

    /**
     * In this test we emulate the following scenario, 3 tables (T0, T1, T2). Only T0 and T1 are replicated,
     * however, transactions are written across the 3 tables. This scenario should fail as invalid tables are
     * crossing transactional boundaries.
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

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Snapshot Sync
        startSnapshotSync(replicateTables);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
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

        testSnapshotSyncCrossTables(crossTables, true);

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

        // Open a dump table
        CorfuTable<Long, Long> dumpDataTable = srcDataRuntime.getObjectsView()
                .build()
                .setStreamName("dumpData")
                .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                })
                .setSerializer(Serializers.PRIMITIVE)
                .open();

        // Generate some dump data to enforce an empty snapshot transfer
        for (long i = 0; i < NUM_KEYS; i++) {
            dumpDataTable.put(i, i);
        }

        // Verify no data on source
        System.out.println("****** Verify No Data in Source Site");
        verifyNoData(srcCorfuTables);

        // Verify destination tables have no actual data.
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        System.out.println("****** Verify No Data in Destination Site");
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
        System.out.println("****** Verify Data in Source Site");
        verifyData(srcCorfuTables, srcDataForVerification);

        // Verify destination tables have no actual data before log replication
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        System.out.println("****** Verify No Data in Destination Site");
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
        System.out.println("****** Verify No Data in Source Site");
        verifyNoData(srcCorfuTables);

        // Verify destination tables have no actual data.
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        System.out.println("****** Verify No Data in Destination Site");
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
        testSnapshotSyncCrossTables(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages = NUM_KEYS * WRITE_CYCLES;
        testConfig.clear();

        startLogEntrySync(crossTables);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
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

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;

        testConfig.clear().setDropMessageLevel(1);
        startLogEntrySync(crossTables, WAIT.ON_ACK);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");

        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Destination
        verifyData(dstCorfuTables, srcDataForVerification);
    }


    @Test
    public void testLogEntrySyncValidCrossTablesWithTriggerTimeout() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;

        testConfig.clear().setDropMessageLevel(2);
        startLogEntrySync(crossTables, WAIT.ON_ERROR);
    }

    /**
     * Test Log Entry Sync, when transactions are performed across invalid tables
     * (i.e., NOT all tables in the transaction are set to be replicated).
     *
     * This test should fail log replication completely as we do not support
     * transactions across federated and non-federated tables.
     *
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncInvalidCrossTables() throws Exception {
        // Write data in transaction to t0, t1 (tables to be replicated) and also include a non-replicated table
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);
        crossTables.add(t2);

        // Writes transactions to t0, t1 and t2 + transactions across 'crossTables'
        testSnapshotSyncCrossTables(crossTables, true);

        Set<String> replicateTables = new HashSet<>();
        replicateTables.add(t0);
        replicateTables.add(t1);

        // Start Log Entry Sync
        // We need to block until the error is received and verify the state machine is shutdown
        testConfig.clear();
        LogReplicationFSM fsm = startLogEntrySync(replicateTables, WAIT.ON_ERROR);

        checkStateChange(fsm, LogReplicationStateType.STOPPED, true);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Destination
        verifyNoData(dstCorfuTables);
    }

    /**
     * Test Log Entry Sync, when transactions are performed across invalid tables
     * (i.e., NOT all tables in the transaction are set to be replicated).
     *
     * This test should fail log replication completely as we do not support
     * transactions across federated and non-federated tables.
     *
     * In this test, we first initiate transactions on valid replicated streams, and then introduce
     * transactions across replicated and non-replicated tables, we verify log entry sync is
     * achieved partially and then stopped due to error.
     *
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncInvalidCrossTablesPartial() throws Exception {
        // Write data in transaction to t0, t1 (tables to be replicated) and also include a non-replicated table
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);
        crossTables.add(t2);

        // Writes transactions to t0, t1 and t2 + transactions across 'crossTables'
        testSnapshotSyncCrossTables(crossTables, false);

        Set<String> replicateTables = new HashSet<>();
        replicateTables.add(t0);
        replicateTables.add(t1);

        // Start Log Entry Sync
        // We need to block until the error is received and verify the state machine is shutdown
        testConfig.clear();
        LogReplicationFSM fsm = startLogEntrySync(replicateTables, WAIT.ON_ERROR);

        checkStateChange(fsm, LogReplicationStateType.STOPPED, true);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");

        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();
        // Add partial transaction entries which were transmitted before transactions across non-replicated streams
        HashMap<String, HashMap<Long, Long>> partialSrcHashMap = new HashMap<>();
        partialSrcHashMap.put(t0, new HashMap<>());
        partialSrcHashMap.put(t1, new HashMap<>());
        for (int i=NUM_KEYS; i<NUM_KEYS*2; i++) {
            partialSrcHashMap.get(t0).put((long)i, (long)i);
            partialSrcHashMap.get(t1).put((long)i, (long)i);
        }

        // Verify Destination
        verifyData(dstCorfuTables, partialSrcHashMap);
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

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;

        testConfig.clear();
        testConfig.setWritingSrc(true);
        testConfig.setDeleteOP(true);

        testConfig.setWaitOn(WAIT.ON_ACK);
        startLogEntrySync(crossTables, WAIT.ON_ACK);

        expectedAckTimestamp = Long.MAX_VALUE;
        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Destination
        verifyData(dstCorfuTables, srcDataForVerification);
        expectedAckTimestamp = srcDataRuntime.getAddressSpaceView().getLogTail();
        assertThat(expectedAckTimestamp).isEqualTo(logReplicationMetadataManager.getLastProcessedLogTimestamp());
        verifyPersistedSnapshotMetadata();
        verifyPersistedLogEntryMetadata();
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
        LogReplicationSourceManager sourceManager = startSnapshotSync(srcCorfuTables.keySet(), WAIT.ON_ERROR);

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

        expectedAckMsgType = MessageType.SNAPSHOT_END;

        LogReplicationSourceManager sourceManager = startSnapshotSync(srcCorfuTables.keySet(),
                new HashSet<>(Arrays.asList(WAIT.ON_ACK, WAIT.ON_ACK_TS, WAIT.ON_ERROR, WAIT.ON_SINK_RECEIVE)));

        // KWrite a checkpoint and trim
        Token token = ckStreamsAndTrim(srcDataRuntime, srcCorfuTables);
        srcDataRuntime.getAddressSpaceView().invalidateServerCaches();
        expectedAckTimestamp = srcDataRuntime.getAddressSpaceView().getLogTail();

        System.out.println("\n****** Wait until an Trimmed Error happens");
        blockUntilExpectedValueReached.acquire();
        System.out.println("\n****** Got an expected Error");

        // Be sure log was trimmed
        while (srcDataRuntime.getAddressSpaceView().getTrimMark().getSequence()
                < token.getSequence()) {
            // no-op
        }

        // Block until snapshot sync is completed (ack is received)
        System.out.println("\n****** Wait until snapshot sync is completed (ack received)");
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
        LogReplicationFSM fsm = startLogEntrySync(srcCorfuTables.keySet(), WAIT.ON_ERROR, true);

        // Verify its in require snapshot sync state and that data was not completely transferred to destination
//        checkStateChange(fsm, LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC, true);
        System.out.println("****** Verify no data at destination");
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
        final int TRIM_RATIO = NUM_KEYS_LARGE - 20;

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

        System.out.println("****** Trim log, will trigger a full snapshot sync");

        Token toke = ckStreamsAndTrim(srcDataRuntime, srcCorfuTables);

        // Be sure log was trimmed
        while (srcDataRuntime.getAddressSpaceView().getTrimMark().getSequence() < toke.getSequence()) {
            // no-op
        }

        System.out.println("****** Wait till IN_SNAPSHOT_SYNC");
        checkStateChange(fsm, LogReplicationStateType.IN_SNAPSHOT_SYNC, true);

        System.out.println("Block untill full snapshot transfer complete");
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
        System.out.println("****** Generate TX Data");
        generateTXData(srcCorfuTables, srcDataForVerification, num_keys, srcDataRuntime, 0);

        // Replicate the only table we created, block until 10 messages are received,
        // then enforce a trim on the log.
        System.out.println("****** Start Log Entry Sync");
        expectedAckMessages = num_keys;
        LogReplicationFSM fsm = startLogEntrySync(srcCorfuTables.keySet(), WAIT.ON_ACK, false);

        System.out.println("****** Total " + num_keys + " ACK messages received. Verify State.");

        // Verify its in log entry sync state and that data was completely transferred to destination
        checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);
        verifyData(dstCorfuTables, dstDataForVerification);
        cleanEnv();
    }


    /* ********************** AUXILIARY METHODS ********************** */


    // startCrossTx indicates if we start with a transaction across Tables
    private void testSnapshotSyncCrossTables(Set<String> crossTableTransactions, boolean startCrossTx) throws Exception {
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
        System.out.println("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);
    }

    void startTxAtSrc() {
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        generateTransactionsCrossTables(srcCorfuTables, crossTables, srcDataForVerification,
                NUM_KEYS_LARGE, srcDataRuntime, NUM_KEYS*WRITE_CYCLES);
    }

    void startTxDst() {

    }


    private void trim(CorfuRuntime rt, int trimAddress) {
        System.out.println("Trim at: " + trimAddress);
        rt.getAddressSpaceView().prefixTrim(new Token(0, trimAddress));
        rt.getAddressSpaceView().invalidateServerCaches();
    }

    /**
     * It will start to continue pump data to src or dst according to the config.
     */
    void startTx() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
        if (testConfig.writingSrc) {
            expectedAckMessages += NUM_KEYS_LARGE;
            scheduledExecutorService.submit(this::startTxAtSrc);
        }
        if (testConfig.writingDst) {
            scheduledExecutorService.submit(this::startTxDst);
        }
    }

    private LogReplicationSourceManager startSnapshotSync(Set<String> tablesToReplicate) throws Exception {
        return startSnapshotSync(tablesToReplicate, WAIT.ON_ACK);
    }

    private LogReplicationSourceManager startSnapshotSync(Set<String> tablesToReplicate, WAIT waitCondition) throws Exception {
        return startSnapshotSync(tablesToReplicate, new HashSet<>(Arrays.asList(waitCondition)));
    }

    private LogReplicationSourceManager startSnapshotSync(Set<String> tablesToReplicate, Set<WAIT> waitConditions) throws Exception {

        // Observe ACKs on LogReplicationSourceManager, to assess when snapshot sync is completed
        // We only expect one message, related to the snapshot sync complete
        expectedAckMsgType = MessageType.SNAPSHOT_END;
        blockUntilExpectedAckType.acquire();

        LogReplicationSourceManager logReplicationSourceManager = setupSourceManagerAndObservedValues(tablesToReplicate,
                waitConditions);

        // Start Snapshot Sync
        System.out.println("\n****** Start Snapshot Sync");
        logReplicationSourceManager.startSnapshotSync();

        // Block until the wait condition is met (triggered)
        System.out.println("\n****** Wait until wait condition is met " + waitConditions);

        if (waitConditions.contains(WAIT.ON_ERROR)) {
            System.out.print("\n****** blockUnitileExpectedValueReached " + expectedErrors);
            blockUntilExpectedValueReached.acquire();
        } else {
            System.out.print("\n****** blockUnitilExpectedAckType " + expectedAckMsgType);
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
        System.out.println("****** Start Log Entry Sync with src tail " + srcDataRuntime.getAddressSpaceView().getLogTail()
                + " dst tail " + dstDataRuntime.getAddressSpaceView().getLogTail());
        logReplicationSourceManager.startReplication(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_START));

        // Start TX's in parallel, while log entry sync is running
        if (injectTxData) {
            startTx();
        }

        blockUntilExpectedAckTs.acquire();
        expectedAckTimestamp = srcDataRuntime.getAddressSpaceView().getLogTail();

        // Block until the snapshot sync completes == one ACK is received by the source manager, or an error occurs
        System.out.println("\n****** Wait until the wait condition is met");
        if (waitConditions.contains(WAIT.ON_ERROR)) {
            blockUntilExpectedValueReached.acquire();
        } else if (waitConditions.contains(WAIT.ON_ACK)) {
            blockUntilExpectedAckTs.acquire();
        }

        return logReplicationSourceManager.getLogReplicationFSM();
    }

    private LogReplicationSourceManager setupSourceManagerAndObservedValues(Set<String> tablesToReplicate,
                                                                            WAIT waitCondition) throws InterruptedException {
        Set<WAIT> conditions = new HashSet<>();
        conditions.add(waitCondition);
        return setupSourceManagerAndObservedValues(tablesToReplicate, conditions);
    }

    private LogReplicationSourceManager setupSourceManagerAndObservedValues(Set<String> tablesToReplicate,
                                                                            Set<WAIT> waitConditions) throws InterruptedException {
        // Config
        LogReplicationConfig config = new LogReplicationConfig(tablesToReplicate, ACTIVE_CLUSTER_ID, REMOTE_CLUSTER_ID);

        // Data Sender
        sourceDataSender = new SourceForwardingDataSender(DESTINATION_ENDPOINT, config, testConfig.getDropMessageLevel(), logReplicationMetadataManager);

        // Source Manager
        LogReplicationSourceManager logReplicationSourceManager = new LogReplicationSourceManager(readerRuntime, sourceDataSender, config);

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
                    errorsLogEntrySync = sourceDataSender.getErrors();
                    errorsLogEntrySync.addObserver(this);
                    break;
                case ON_DATA_CONTROL_CALL: // Wait on Data Control Calls on Source
                    // TODO (Anny) replace for new logic as In require snapshot sync state is removed
                    break;
                case ON_RESCHEDULE_SNAPSHOT_SYNC: // Wait on calls to reschedule snapshot sync request on Source
                    // TODO (Anny) replace for new logic as In require snapshot sync state is removed
                    break;
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
        if (o == ackMessages) {
            verifyExpectedAckMessage((ObservableAckMsg)o);
        } else if (o == errorsLogEntrySync) {
            verifyExpectedValue(expectedErrors, errorsLogEntrySync.getValue());
        } else if (o == sinkReceivedMessages) {
            verifyExpectedValue(expectedSinkReceivedMessages, sinkReceivedMessages.getValue());
        }
    }

    private void verifyExpectedValue(long expectedValue, long currentValue) {
        // If expected value, release semaphore / unblock the wait
        // System.out.print("\nexpected " + expectedValue + " currentValue " + currentValue);
        if (expectedValue == currentValue) {
            if (expectedAckMsgType != null) {
                blockUntilExpectedValueReached.release();
            }
        }
    }

    private void verifyExpectedAckMessage(ObservableAckMsg observableAckMsg) {
        // If expected a ackTs, release semaphore / unblock the wait
        if (observableAckMsg.getDataMessage() != null) {
            LogReplicationEntry logReplicationEntry = observableAckMsg.getDataMessage();
            //System.out.print("\nackMsg " + logReplicationEntry.getMetadata());
            switch (testConfig.waitOn) {
                case ON_ACK:
                    verifyExpectedValue(expectedAckMessages, ackMessages.getMsgCnt());
                case ON_ACK_TS:
                    verifyExpectedValue(expectedAckTimestamp, logReplicationEntry.getMetadata().timestamp);
                    if (expectedAckMsgType == logReplicationEntry.getMetadata().getMessageMetadataType()) {
                        blockUntilExpectedAckType.release();
                    }

                    if (expectedAckTimestamp == logReplicationEntry.getMetadata().timestamp) {
                        blockUntilExpectedAckTs.release();
                    }
                    break;
            }
        }
    }

    private void verifyPersistedSnapshotMetadata() {
        long lastSnapStart = logReplicationMetadataManager.getLastSnapStartTimestamp();
        long lastSnapDone = logReplicationMetadataManager.getLastSrcBaseSnapshotTimestamp();

        System.out.println("\nlastSnapStart " + lastSnapStart + " lastSnapDone " + lastSnapDone);
        assertThat(lastSnapStart == lastSnapDone).isTrue();
    }

    private void verifyPersistedLogEntryMetadata() {
        long lastLogProcessed = logReplicationMetadataManager.getLastProcessedLogTimestamp();

        System.out.println("\nlastLogProcessed " + lastLogProcessed + " expectedTimestamp " + expectedAckTimestamp);
        assertThat(expectedAckTimestamp == lastLogProcessed).isTrue();
    }

    public enum WAIT {
        ON_ACK,
        ON_ACK_TS,
        ON_ERROR,
        ON_DATA_CONTROL_CALL,
        ON_RESCHEDULE_SNAPSHOT_SYNC,
        ON_SINK_RECEIVE,
        //ON_SNAPSHOT_END,
        NONE
    }

    @Data
    public static class TestConfig {
        int dropMessageLevel = 0;
        boolean trim = false;
        boolean writingSrc = false;
        boolean writingDst = false;
        boolean deleteOP = false;
        WAIT waitOn = WAIT.ON_ACK;

        public TestConfig() {}

        public TestConfig clear() {
            dropMessageLevel = 0;
            trim = false;
            writingSrc = false;
            writingDst = false;
            deleteOP = false;
            return this;
        }
    }
}
