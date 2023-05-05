package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValue;
import org.corfudb.infrastructure.logreplication.proto.Sample.Metadata;
import org.corfudb.infrastructure.logreplication.proto.Sample.StringKey;
import org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.StreamsSnapshotWriter;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsLogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsSnapshotReader;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class LogReplicationReaderWriterIT extends AbstractIT {
    private static final String DEFAULT_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    private static final int WRITER_PORT = DEFAULT_PORT + 1;
    private static final String WRITER_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;
    private static final int START_VAL = 11;
    private static final int NUM_KEYS = 10;
    private static final int NUM_STREAMS = 2;
    private static final int NUM_TRANSACTIONS = 20;
    private static final String SOURCE_CLUSTER_ID = "Cluster-Paris";
    private static final String SINK_CLUSTER_ID = "Cluster-London";
    private static final String SHADOW_SUFFIX = "_SHADOW";
    private static final String TEST_NAMESPACE = "LR-Test";
    private static final String LOCAL_SOURCE_CLUSTER_ID = DefaultClusterConfig.getSourceClusterIds().get(0);
    private static final String LOCAL_SINK_CLUSTER_ID = DefaultClusterConfig.getSinkClusterIds().get(0);

    private static Semaphore waitSem = new Semaphore(1);

    private static final UUID snapshotSyncId = UUID.randomUUID();

    // Connect with server1 to generate data
    private CorfuRuntime srcDataRuntime = null;

    // Connect with server2 to verify data
    private CorfuRuntime dstDataRuntime = null;

    private final Map<String, Table<StringKey, IntValue, Metadata>> srcTables = new HashMap<>();
    private final Map<String, Table<StringKey, IntValue, Metadata>> dstTables = new HashMap<>();
    private final Map<String, Table<StringKey, IntValue, Metadata>> shadowTables = new HashMap<>();

    private CorfuStore srcCorfuStore;

    private CorfuStore dstCorfuStore;

    // Corfu tables in-memory view, used for verification.
    private final Map<String, Map<String, Integer>> srcHashMap = new HashMap<>();

    // Store messages generated by stream snapshot reader and will play it at the writer side.
    private final List<LogReplicationEntryMsg> msgQ = new ArrayList<>();

    private void setupEnv() throws IOException {
        new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        srcDataRuntime = CorfuRuntime.fromParameters(params);
        srcDataRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        srcDataRuntime.connect();

        dstDataRuntime = CorfuRuntime.fromParameters(params);
        dstDataRuntime.parseConfigurationString(WRITER_ENDPOINT);
        dstDataRuntime.connect();

        srcCorfuStore = new CorfuStore(srcDataRuntime);
        dstCorfuStore = new CorfuStore(dstDataRuntime);
    }

    public static void openStreams(Map<String, Table<StringKey, IntValue, Metadata>> tables, CorfuStore corfuStore)
            throws Exception {
        openStreams(tables, corfuStore, NUM_STREAMS, Serializers.PRIMITIVE, false);
    }

    public static void openStreams(Map<String, Table<StringKey, IntValue, Metadata>> tables, CorfuStore corfuStore,
                                   int num_streams, ISerializer serializer) throws Exception {
        openStreams(tables, corfuStore, num_streams, serializer, false);
    }

    public static void openStreams(Map<String, Table<StringKey, IntValue, Metadata>> tables, CorfuStore corfuStore,
                                   int num_streams, ISerializer serializer, boolean shadow) throws Exception {
        for (int i = 0; i < num_streams; i++) {
            String name = "test" + i;
            if (shadow) {
                name = name + SHADOW_SUFFIX;
            }
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

    public static void generateData(Map<String, Table<StringKey, IntValue, Metadata>> tables,
                      Map<String, Map<String, Integer>> hashMap,
                      int numKeys, CorfuStore corfuStore, int startVal) {
        for (int i = 0; i < numKeys; i++) {
            for (String name : tables.keySet()) {
                int key = i + startVal;
                try (TxnContext txn = corfuStore.txn(TEST_NAMESPACE)) {
                    txn.putRecord(tables.get(name), StringKey.newBuilder().setKey(String.valueOf(key)).build(),
                            IntValue.newBuilder().setValue(key).build(), null);
                    txn.commit();
                }

                hashMap.putIfAbsent(name, new HashMap<>());
                hashMap.get(name).put(String.valueOf(key), key);
            }
        }
    }

    // Generate data with transactions and the same time push the data to the hashtable
    public static void generateTransactions(Map<String, Table<StringKey, IntValue, Metadata>> tables,
                      Map<String, Map<String, Integer>> hashMap,
                      int numT, CorfuStore corfuStore, int startVal) {
        int j = 0;
        for (int i = 0; i < numT; i++) {
            for (String name : tables.keySet()) {
                hashMap.putIfAbsent(name, new HashMap<>());
                int key = j + startVal;
                try (TxnContext txn = corfuStore.txn(TEST_NAMESPACE)) {
                    txn.putRecord(tables.get(name), StringKey.newBuilder().setKey(String.valueOf(key)).build(),
                            IntValue.newBuilder().setValue(key).build(), null);
                    txn.commit();
                }

                hashMap.putIfAbsent(name, new HashMap<>());
                hashMap.get(name).put(String.valueOf(key), key);
                j++;
            }
        }
        log.debug("Generate transactions num " + numT);
    }

    public static void verifyData(String tag, Map<String, Table<StringKey, IntValue, Metadata>> tables,
                                  Map<String, Map<String, Integer>> hashMap, CorfuStore corfuStore) {
        log.debug("\n" + tag);
        for (String name : hashMap.keySet()) {
            Table<StringKey, IntValue, Metadata> table = tables.get(name);
            Map<String, Integer> mapKeys = hashMap.get(name);
            log.debug("table " + name + " key size " + table.count() +
                    " hashMap size " + mapKeys.size());

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

    public static void printTails(String tag, CorfuRuntime rt0, CorfuRuntime rt1) {
        log.debug("\n" + tag);
        log.debug("src dataTail " + rt0.getAddressSpaceView().getLogTail());
        log.debug("dst dataTail " + rt1.getAddressSpaceView().getLogTail());

    }

    public static void readSnapshotMsgs(List<LogReplicationEntryMsg> msgQ, CorfuRuntime rt, boolean blockOnSem) {
        int cnt = 0;
        LogReplicationConfigManager configManager = new LogReplicationConfigManager(rt, LOCAL_SOURCE_CLUSTER_ID);
        configManager.generateConfig(Collections.singleton(getDefaultSession()));
        LogReplicationContext context = new LogReplicationContext(configManager, 0, DEFAULT_ENDPOINT,
                Mockito.mock(LogReplicationPluginConfig.class));

        StreamsSnapshotReader reader = new StreamsSnapshotReader(rt, getDefaultSession(), context);

        reader.reset(rt.getAddressSpaceView().getLogTail());
        while (true) {
            cnt++;

            SnapshotReadMessage snapshotReadMessage = reader.read(snapshotSyncId);
            for (LogReplicationEntryMsg data : snapshotReadMessage.getMessages()) {
                msgQ.add(data);
                log.debug("generate msg " + cnt);
            }

            if (snapshotReadMessage.isEndRead()) {
                break;
            }

            if  (blockOnSem) {
                try {
                    waitSem.acquire();
                } catch (InterruptedException e) {
                    log.info("Caught an interrupted exception ", e);
                }
                blockOnSem = false;
            }
        }
    }

    public void writeSnapshotMsgs(List<LogReplicationEntryMsg> msgQ, CorfuRuntime rt) {

        LogReplicationConfigManager configManager = new LogReplicationConfigManager(rt, LOCAL_SINK_CLUSTER_ID);
        configManager.generateConfig(Collections.singleton(getDefaultSession()));

        LogReplicationMetadataManager logReplicationMetadataManager = new LogReplicationMetadataManager(rt,
                getReplicationContext(configManager, 0, "test", true));
        logReplicationMetadataManager.addSession(getDefaultSession(), 0, true);

        StreamsSnapshotWriter writer = new StreamsSnapshotWriter(rt, logReplicationMetadataManager,
            getDefaultSession(), new LogReplicationContext(configManager, 0, DEFAULT_ENDPOINT,
                Mockito.mock(LogReplicationPluginConfig.class)));

        if (msgQ.isEmpty()) {
            log.debug("msgQ is empty");
        }

        long topologyConfigId = msgQ.get(0).getMetadata().getTopologyConfigID();
        long snapshot = msgQ.get(0).getMetadata().getSnapshotTimestamp();
        logReplicationMetadataManager.updateReplicationMetadataField(getDefaultSession(),
                LogReplicationMetadata.ReplicationMetadata.LASTSNAPSHOTSTARTED_FIELD_NUMBER, snapshot);
        writer.reset(topologyConfigId, snapshot);

        for (LogReplicationEntryMsg msg : msgQ) {
            writer.apply(msg);
        }

        writer.applyShadowStreams();
    }

    public static LogReplicationSession getDefaultSession() {
        return LogReplicationSession.newBuilder()
                .setSinkClusterId(SINK_CLUSTER_ID)
                .setSourceClusterId(SOURCE_CLUSTER_ID)
                .setSubscriber(LogReplicationConfigManager.getDefaultSubscriber())
                .build();
    }

    public static void readLogEntryMsgs(List<LogReplicationEntryMsg> msgQ, CorfuRuntime rt,
                                        boolean blockOnce) throws TrimmedException {

        LogReplicationConfigManager configManager = new LogReplicationConfigManager(rt, LOCAL_SOURCE_CLUSTER_ID);
        configManager.generateConfig(Collections.singleton(getDefaultSession()));

        StreamsLogEntryReader reader = new StreamsLogEntryReader(rt, getDefaultSession(),
                new LogReplicationContext(configManager, 0, DEFAULT_ENDPOINT,
                        Mockito.mock(LogReplicationPluginConfig.class)));
        reader.setGlobalBaseSnapshot(Address.NON_ADDRESS, Address.NON_ADDRESS);

        LogReplicationEntryMsg entry;

        do {
            entry = reader.read(UUID.randomUUID());

            if (entry != null) {
                msgQ.add(entry);
            }

            if (blockOnce) {
                try {
                    waitSem.acquire();
                } catch (InterruptedException e) {
                    log.info("Caught an InterruptedException ", e);
                }
                blockOnce = false;
            }

            log.debug(" msgQ size " + msgQ.size());

        } while (entry != null);

        assertThat(reader.getLastOpaqueEntry()).isNull();
    }

    private void writeLogEntryMsgs(List<LogReplicationEntryMsg> msgQ, CorfuRuntime rt) {

        LogReplicationConfigManager configManager = new LogReplicationConfigManager(rt, LOCAL_SINK_CLUSTER_ID);
        configManager.generateConfig(Collections.singleton(getDefaultSession()));

        LogReplicationMetadataManager logReplicationMetadataManager = new LogReplicationMetadataManager(rt,
                getReplicationContext(configManager, 0, "test", true));
        logReplicationMetadataManager.addSession(getDefaultSession(),0, true);
        LogEntryWriter writer = new LogEntryWriter(logReplicationMetadataManager, getDefaultSession(),
                new LogReplicationContext(configManager, 0, DEFAULT_ENDPOINT,
                        Mockito.mock(LogReplicationPluginConfig.class)));

        if (msgQ.isEmpty()) {
            log.debug("msgQ is EMPTY");
        }

        for (LogReplicationEntryMsg msg : msgQ) {
            writer.apply(msg);
        }
    }

    private LogReplicationContext getReplicationContext(LogReplicationConfigManager configManager, long topologyConfigId,
                                                        String localCorfuEndpoint, boolean isLeader) {
        return new LogReplicationContext(configManager, topologyConfigId, localCorfuEndpoint, isLeader,
                Mockito.mock(LogReplicationPluginConfig.class));
    }

    private void accessTxStream(Iterator<ILogData> iterator, int num) {
        int i = 0;
        while (iterator.hasNext() && i++ < num) {
            iterator.next();
        }
    }

    public static void trimAlone(long address, CorfuRuntime rt) {
        // Trim the log
        Token token = new Token(0, address);
        rt.getAddressSpaceView().prefixTrim(token);
        rt.getAddressSpaceView().gc();
        rt.getAddressSpaceView().invalidateServerCaches();
        rt.getAddressSpaceView().invalidateClientCache();
        waitSem.release();
        log.debug("\ntrim at " + token + " currentTail " + rt.getAddressSpaceView().getLogTail());
    }

    /**
     * enforce checkpoint entries at the streams.
     */
    public static Token ckStreamsAndTrim(CorfuRuntime rt, Map<String, Table<StringKey, IntValue, Metadata>> tables) {
        MultiCheckpointWriter<CorfuTable<StringKey, CorfuRecord<IntValue, Metadata>>> mcw1 = new MultiCheckpointWriter<>();
        for (Table<StringKey, IntValue, Metadata> map : tables.values()) {
            CorfuTable<StringKey, CorfuRecord<IntValue, Metadata>> table = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<StringKey, CorfuRecord<IntValue, Metadata>>>() {})
                    .setStreamName(map.getFullyQualifiedTableName())
                    .open();
            mcw1.addMap(table);
        }

        Token checkpointAddress = mcw1.appendCheckpoints(rt, "test");

        // Trim the log
        trimAlone(checkpointAddress.getSequence(), rt);
        return checkpointAddress;
    }

    public void checkpointAndTrim(CorfuRuntime rt) {
        Token token = ckStreamsAndTrim(rt, srcTables);

        Token trimMark = rt.getAddressSpaceView().getTrimMark();

        while (trimMark.getSequence() != (token.getSequence() + 1)) {
            log.debug("trimMark " + trimMark + " trimToken " + token);
            trimMark = rt.getAddressSpaceView().getTrimMark();
        }

        rt.getAddressSpaceView().invalidateServerCaches();
        log.debug("trim " + token);
    }

    private void trimDelay() {
        try {
            while(msgQ.isEmpty()) {
                TimeUnit.MILLISECONDS.sleep(1);
            }
            checkpointAndTrim(srcDataRuntime);
        } catch (Exception e) {
            log.debug("caught an exception " + e);
        }
    }

    private void trimAloneDelay() {
        try {
            while(msgQ.isEmpty()) {
                TimeUnit.MILLISECONDS.sleep(1);
            }
            trimAlone(srcDataRuntime.getAddressSpaceView().getLogTail(), srcDataRuntime);
        } catch (Exception e) {
            log.debug("caught an exception " + e);
        }
    }

    /**
     * Generate some transactions, and start a txstream. Do a trim
     * To see if a trimmed exception happens
     */
    @Test
    public void testTrimmedExceptionForTxStream() throws Exception {
        setupEnv();
        openStreams(srcTables, srcCorfuStore);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcCorfuStore, NUM_KEYS);

        // Open the log replication stream
        IStreamView txStream = srcDataRuntime.getStreamsView().get(ObjectsView.getLogReplicatorStreamId());
        long tail = srcDataRuntime.getAddressSpaceView().getLogTail();
        Stream<ILogData> stream = txStream.streamUpTo(tail);
        Iterator<ILogData> iterator = stream.iterator();

        Exception result = null;
        checkpointAndTrim(srcDataRuntime);

        try {
            accessTxStream(iterator, (int)tail);
        } catch (Exception e) {
            result = e;
            log.debug("caught an exception " + e + " tail " + tail);
        } finally {
            assertThat(result).isInstanceOf(TrimmedException.class);
        }
    }

    @Test
    public void testOpenTableAfterTrimWithoutCheckpoint() throws Exception {
        final int offset = 20;
        setupEnv();
        openStreams(srcTables, srcCorfuStore);
        generateTransactions(srcTables, srcHashMap, NUM_KEYS, srcCorfuStore, NUM_KEYS);

        trimAlone(srcDataRuntime.getAddressSpaceView().getLogTail() - offset, srcDataRuntime);

        try {
            CorfuTable<Long, Long> testTable = srcDataRuntime.getObjectsView()
                    .build()
                    .setStreamName("test0")
                    .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                    })
                    .setSerializer(Serializers.PRIMITIVE)
                    .open();
            testTable.size();
        } catch (Exception e) {
            log.debug("caught a exception " + e);
            assertThat(e).isInstanceOf(TrimmedException.class);
        }
    }

    @Test
    public void testTrimmedExceptionForLogEntryReader() throws Exception {
        setupEnv();

        waitSem = new Semaphore(1);
        waitSem.acquire();

        openStreams(srcTables, srcCorfuStore);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcCorfuStore, NUM_KEYS);
        long tail = srcDataRuntime.getAddressSpaceView().getLogTail();

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.submit(this::trimAloneDelay);
        Exception result = null;

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        CorfuRuntime srcReaderRuntime = CorfuRuntime.fromParameters(params);
        srcReaderRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        srcReaderRuntime.connect();

        try {
            readLogEntryMsgs(msgQ, srcReaderRuntime, true);
        } catch (Exception e) {
            result = e;
            assertThat(result).isInstanceOf(TransactionAbortedException.class);
            assertThat(result.getCause()).isInstanceOf(TrimmedException.class);
            log.debug("msgQ size " + msgQ.size());
            log.debug("caught an exception " + e + " tail " + tail);
        }

        tearDownEnv();
        cleanUp();
    }

    private void tearDownEnv() {
        if (srcDataRuntime != null) {
            srcDataRuntime.shutdown();
            dstDataRuntime.shutdown();
        }
    }

    @Test
    public void testTrimmedExceptionForSnapshotReader() throws Exception {
        setupEnv();
        waitSem = new Semaphore(1);
        waitSem.acquire();
        openStreams(srcTables, srcCorfuStore, 1, Serializers.PRIMITIVE);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcCorfuStore, NUM_KEYS);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.submit(this::trimDelay);

        long tail = srcDataRuntime.getAddressSpaceView().getLogTail();
        Exception result = null;

        try {
            readSnapshotMsgs(msgQ, srcDataRuntime, true);
        } catch (Exception e) {
            result = e;
            log.debug("caught an exception " + e + " tail " + tail);
        } finally {
            log.debug("msgQ size " + msgQ.size());
            assertThat(result).isInstanceOf(TrimmedException.class);
        }
    }


    /**
     * Write to a corfu table and read SMRntries with streamview,
     * redirect the SMRentries to the second corfu server, and verify
     * the second corfu server contains the correct <key, value> pairs
     * @throws Exception
     */
    @Test
    public void testWriteSMREntries() throws Exception {
        // setup environment
        log.debug("\ntest start");
        setupEnv();

        openStreams(srcTables, srcCorfuStore);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcCorfuStore, NUM_KEYS);
        verifyData("after writing to server1", srcTables, srcHashMap, srcCorfuStore);
        printTails("after writing to server1", srcDataRuntime, dstDataRuntime);

        //read streams as SMR entries
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        CorfuRuntime srcReaderRuntime = CorfuRuntime.fromParameters(params);
        srcReaderRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        srcReaderRuntime.connect();


        CorfuRuntime dstWriterRuntime = CorfuRuntime.fromParameters(params);
        dstWriterRuntime.parseConfigurationString(WRITER_ENDPOINT);
        dstWriterRuntime.connect();

        for (String name : srcHashMap.keySet()) {
            IStreamView srcSV = srcReaderRuntime.getStreamsView().getUnsafe(CorfuRuntime.getStreamID(name), options);
            IStreamView dstSV = dstWriterRuntime.getStreamsView().getUnsafe(CorfuRuntime.getStreamID(name), options);

            List<ILogData> dataList = srcSV.remaining();
            for (ILogData data : dataList) {
                OpaqueEntry opaqueEntry = OpaqueEntry.unpack(data);
                for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                    for (SMREntry entry : opaqueEntry.getEntries().get(uuid)) {
                        dstSV.append(entry);
                    }
                }
            }
        }

        printTails("after writing to dst", srcDataRuntime, dstDataRuntime);
        openStreams(dstTables, dstCorfuStore);
        verifyData("after writing to dst", dstTables, srcHashMap, dstCorfuStore);
    }

    @Test
    public void testSnapshotTransfer() throws Exception {
        setupEnv();

        openStreams(srcTables, srcCorfuStore);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcCorfuStore, START_VAL);
        verifyData("after writing to src", srcTables, srcHashMap, srcCorfuStore);

        // generate dump data at dst
        openStreams(dstTables, dstCorfuStore);

        // read snapshot from srcServer and put msgs into Queue
        readSnapshotMsgs(msgQ, srcDataRuntime, false);

        // play messages at dst server
        writeSnapshotMsgs(msgQ, dstDataRuntime);

        printTails("after writing to server2", srcDataRuntime, dstDataRuntime);

        // Verify data with hashtable
        verifyData("after snap write at dst", dstTables, srcHashMap, dstCorfuStore);
    }

    @Test
    public void testLogEntryTransferWithSerializer() throws Exception {
        setupEnv();
        ISerializer serializer = new TestSerializer(Byte.MAX_VALUE);

        openStreams(srcTables, srcCorfuStore, NUM_STREAMS, serializer);
        openStreams(shadowTables, dstCorfuStore, NUM_STREAMS, serializer, true);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcCorfuStore, NUM_TRANSACTIONS);

        //read snapshot from srcServer and put msgs into Queue
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        CorfuRuntime srcReaderRuntime = CorfuRuntime.fromParameters(params);
        srcReaderRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        srcReaderRuntime.connect();
        readLogEntryMsgs(msgQ, srcReaderRuntime, false);

        CorfuRuntime dstWriterRuntime = CorfuRuntime.fromParameters(params);
        dstWriterRuntime.parseConfigurationString(WRITER_ENDPOINT);
        dstWriterRuntime.connect();
        //play messages at dst server
        writeLogEntryMsgs(msgQ, dstWriterRuntime);

        //verify data with hashtable
        openStreams(dstTables, dstCorfuStore, NUM_STREAMS, serializer);

        dstDataRuntime.getSerializers().registerSerializer(serializer);
        verifyData("after log writing at dst", dstTables, srcHashMap, dstCorfuStore);

        cleanUp();
    }

    /**
     * This test verifies that the Log Entry Reader sets the last processed entry
     * as NULL whenever all entries written to the TX stream are of no interest for
     * the replication process (streams present are not intended for replication)
     *
     * If the lastProcessedEntry is not NULL, this ensures that LogEntryReader will not loop
     * forever on the last observed entry.
     *
     * @throws Exception
     */
    @Test
    public void testLogEntryReaderWhenTxStreamNoStreamsToReplicate() throws Exception {
        setupEnv();

        openStreams(srcTables, srcCorfuStore);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcCorfuStore, START_VAL);
        verifyData("after writing to src", srcTables, srcHashMap, srcCorfuStore);

        // Confirm we get out of Log Entry Reader when there are streams of no interest in the Tx Stream
        readLogEntryMsgs(msgQ, srcDataRuntime, false);

        cleanUp();
    }
}

