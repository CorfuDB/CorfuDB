package org.corfudb.infrastructure.logreplication;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsLogEntryReader;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Slf4j
public class LogEntryReaderTest extends AbstractViewTest {
    private LogEntryReader logEntryReader;

    private CorfuStore corfuStore;

    private final String namespace = "testNamespace";

    private final String tableName1 = "Table1";
    private Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Sample.Metadata> table1;

    private final String tableName2 = "Table2";
    private Table<Sample.StringKey, SampleSchema.SampleTableAMsg, Sample.Metadata> table2;

    @Before
    public void setup() throws Exception {
        corfuStore = new CorfuStore(getDefaultRuntime());
        table1 = corfuStore.openTable(namespace, tableName1, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, Sample.Metadata.class,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        table2 = corfuStore.openTable(namespace, tableName2, Sample.StringKey.class,
            SampleSchema.SampleTableAMsg.class, Sample.Metadata.class,
            TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class));
    }


    /**
     * This test creates delta messages within the maxWriteSize threshold and verifies that all are sent and in the
     * same order.
     * Deltas are generated using the private method generateDeltas() and it was empirically determined that write to
     * a single table has ~185 bytes serialized size.
     */
    @Test
    public void testMessagesAllInLimit() {
        // Configure the maxWriteSize to be DEFAULT_MAX_MSG_BATCH_SIZE and no delta is beyond this threshold.
        verifyAllMessagesSent(LogReplicationConfig.DEFAULT_MAX_MSG_BATCH_SIZE, new HashSet<>());
    }

    /**
     * This test creates delta messages above the maxWriteSize threshold and verifies that all are sent and in the
     * same order.
     * Deltas are generated using the private method generateDeltas() and it was empirically determined that write to
     * a single table has ~185 bytes serialized size.
     */
    @Test
    public void testMessagesAllAboveLimit() {
        // Configure the maxWriteSize to be 100.
        int maxMsgSize = 100;

        // With each delta being ~185 bytes, all will be above the maxMsgSize threshold.
        verifyAllMessagesSent(maxMsgSize, new HashSet<>());
    }

    /**
     * This test creates the first delta message above the maxWriteSize threshold and all others within the
     * threshold.  It verifies that all are sent and in the same order.
     * Deltas are generated using the private method generateDeltas() and it was empirically determined that write to
     * a single table has ~185 serialized size.
     */
    @Test
    public void testMessagesFirstAboveLimit() {
        // With serialized size of 185 bytes, a maxMsgSize of 300 is sufficient.
        // As the first delta must be above 300 bytes, add index 0 to indexAboveLimit set.
        int maxMsgSize = 300;
        Set<Integer> indexAboveLimit = new HashSet<>();
        indexAboveLimit.add(0);
        verifyAllMessagesSent(maxMsgSize, indexAboveLimit);
    }

    /**
     * This test creates the 5th delta message above the maxWriteSize threshold and all others within
     * the threshold.  It verifies that all are sent and in the same order.
     * Deltas are generated using the private method generateDeltas() and it was empirically determined that write to
     * a single table has ~185 serialized size.
     */
    @Test
    public void testMessagesLaterAboveLimit() {
        // With serialized size of 185 bytes, a maxMsgSize of 300 is sufficient.
        // As the fifth delta must be above 300 bytes, add index 5 to indexAboveLimit set.
        int maxMsgSize = 300;
        Set<Integer> indexAboveLimit = new HashSet<>();
        indexAboveLimit.add(5);
        verifyAllMessagesSent(maxMsgSize, indexAboveLimit);
    }

    private void verifyAllMessagesSent(int maxMsgSize, Set<Integer> indexesAboveLimit) {
        createReplicationConfig(maxMsgSize);

        // Generate deltas such that indexes in indexAboveLimit are bigger than maxMsgSize.
        List<Long> seqNumbersWritten = generateDeltas(indexesAboveLimit);

        // Start reading the deltas from LogEntryReader and collect the messages constructed
        UUID requestId = UUID.randomUUID();
        LogReplication.LogReplicationEntryMsg msgCreated;
        List<LogReplication.LogReplicationEntryMsg> msgsSent = new ArrayList<>();

        do {
            msgCreated = logEntryReader.read(requestId);
            if (msgCreated != null) {
                msgsSent.add(msgCreated);
            }
        } while (msgCreated != null);

        // Collect the sequence numbers of all constructed messages
        List<Long> seqNumbersSent = new ArrayList<>();
        for (LogReplication.LogReplicationEntryMsg msg : msgsSent) {
            for (OpaqueEntry opaqueEntry : CorfuProtocolLogReplication.extractOpaqueEntries(msg)) {
                seqNumbersSent.add(opaqueEntry.getVersion());
            }
        }
        // Verify that all addresses which were written are included in the LogEntryMsgs constructed by LogEntryReader.
        Assert.assertEquals(seqNumbersWritten, seqNumbersSent);
    }

    private void createReplicationConfig(int maxMsgSize) {
        CorfuRuntime corfuRuntime = getNewRuntime(getDefaultNode()).connect();
        LogReplicationConfigManager configManager = new LogReplicationConfigManager(corfuRuntime);
        LogReplicationConfig config = new LogReplicationConfig(configManager,
            LogReplicationConfig.DEFAULT_MAX_NUM_MSG_PER_BATCH,
            maxMsgSize, LogReplicationConfig.MAX_CACHE_NUM_ENTRIES,
            LogReplicationConfig.DEFAULT_MAX_SNAPSHOT_ENTRIES_APPLIED);
        config.syncWithRegistry();
        logEntryReader = new StreamsLogEntryReader(corfuRuntime, config);
    }

    private List<Long> generateDeltas(Set<Integer> indexesAboveLimit) {
        // Create deltas by writing to testTable1.  If a delta must be larger than maxMsgSize, write to testTable2
        // in addition to testTable1.
        List<Long> seqNums = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            try (TxnContext txn = corfuStore.txn(namespace)) {
                Sample.StringKey key = Sample.StringKey.newBuilder().setKey("key_" + i).build();
                SampleSchema.ValueFieldTagOne val =
                    SampleSchema.ValueFieldTagOne.newBuilder().setPayload("val_" + i).build();
                txn.putRecord(table1, key, val, null);

                if (indexesAboveLimit.contains(i)) {
                    SampleSchema.SampleTableAMsg sampleTableAMsg =
                        SampleSchema.SampleTableAMsg.newBuilder().setPayload("val_" + i).build();
                    txn.putRecord(table2, key, sampleTableAMsg, null);
                }
                seqNums.add(txn.commit().getSequence());
            }
        }
        return seqNums;
    }
}
