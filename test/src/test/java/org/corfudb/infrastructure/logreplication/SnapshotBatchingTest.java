package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsSnapshotReader;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleSchema;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/** Exercises real stored transactions through the snapshot reader's wire messages. */
public class SnapshotBatchingTest extends AbstractViewTest {
    private static final String NAMESPACE = "snapshot_batch_budget";
    private static final String TABLE = "data";
    private final UUID streamId = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE, TABLE));
    private CorfuStore store;
    private Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Sample.Metadata> table;
    private LogReplicationConfig config;

    @Before
    public void setup() throws Exception {
        store = new CorfuStore(getDefaultRuntime());
        table = store.openTable(NAMESPACE, TABLE, Sample.StringKey.class, SampleSchema.ValueFieldTagOne.class,
                Sample.Metadata.class, TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));
        config = mock(LogReplicationConfig.class);
        when(config.getStreamsToReplicate()).thenReturn(Set.of(TableRegistry.getFullyQualifiedTableName(NAMESPACE, TABLE)));
        when(config.getMaxMsgSize()).thenReturn(100000);
        when(config.getMaxDataSizePerMsg()).thenReturn(90000);
    }

    private long writePair(String key, int valueSize) {
        try (TxnContext txn = store.txn(NAMESPACE)) {
            for (int i = 0; i < 2; i++) {
                txn.putRecord(table, Sample.StringKey.newBuilder().setKey(key + i).build(),
                        SampleSchema.ValueFieldTagOne.newBuilder().setPayload("v".repeat(valueSize)).build(), null);
            }
            return txn.commit().getSequence();
        }
    }

    private List<LogReplicationEntryMsg> read(StreamsSnapshotReader reader) {
        reader.reset(getDefaultRuntime().getAddressSpaceView().getLogTail());
        List<LogReplicationEntryMsg> messages = new ArrayList<>();
        UUID attempt = UUID.randomUUID();
        for (int i = 0; i < 20; i++) {
            SnapshotReadMessage next = reader.read(attempt);
            messages.addAll(next.getMessages());
            if (next.isEndRead()) {
                for (int seq = 0; seq < messages.size(); seq++) {
                    assertEquals(seq, messages.get(seq).getMetadata().getSnapshotSyncSeqNum());
                    assertEquals(attempt, org.corfudb.protocols.CorfuProtocolCommon
                            .getUUID(messages.get(seq).getMetadata().getSyncRequestId()));
                }
                return messages;
            }
        }
        throw new AssertionError("Snapshot reader did not finish");
    }

    private void assertPair(LogReplicationEntryMsg message, long sourceTransaction) {
        var entries = CorfuProtocolLogReplication.extractOpaqueEntries(message);
        assertEquals(1, entries.size());
        assertEquals(sourceTransaction, entries.get(0).getVersion());
        assertEquals(2, entries.get(0).getEntries().get(streamId).size());
    }

    @Test
    public void smallerSinkBudgetSeparatesTransactionsWithoutLosingTheirOrderOrAtomicity() {
        List<Long> versions = List.of(writePair("a", 1000), writePair("b", 1000), writePair("c", 1000));
        StreamsSnapshotReader reader = new StreamsSnapshotReader(getDefaultRuntime(), config);
        assertEquals(1, read(reader).size());
        reader.setSnapshotBatchSizeHint(3000);
        List<LogReplicationEntryMsg> messages = read(reader);
        assertEquals(3, messages.size());
        for (int i = 0; i < messages.size(); i++) {
            assertPair(messages.get(i), versions.get(i));
        }
    }

    @Test
    public void aSingleSourceTransactionIsNotSplitToMeetTheBatchTarget() {
        long large = writePair("large", 6000);
        long small = writePair("small", 100);
        StreamsSnapshotReader reader = new StreamsSnapshotReader(getDefaultRuntime(), config);
        reader.setSnapshotBatchSizeHint(3000);
        List<LogReplicationEntryMsg> messages = read(reader);
        assertEquals(2, messages.size());
        assertPair(messages.get(0), large);
        assertPair(messages.get(1), small);
        assertEquals(Integer.valueOf(1), reader.getObserveBiggerMsg().getValue());
    }

    @Test
    public void unknownBudgetsRestoreLegacyBatchingAndTinyBudgetsStillMakeProgress() {
        writePair("a", 1000);
        writePair("b", 1000);
        StreamsSnapshotReader reader = new StreamsSnapshotReader(getDefaultRuntime(), config);
        reader.setSnapshotBatchSizeHint(1);
        assertEquals(2, read(reader).size());
        reader.setSnapshotBatchSizeHint(0);
        assertEquals(1, read(reader).size());
        reader.setSnapshotBatchSizeHint(-1);
        assertEquals(1, read(reader).size());
    }

    @Test
    public void aLargeSinkBudgetCannotIncreaseTheConfiguredTransportBatchTarget() {
        when(config.getMaxDataSizePerMsg()).thenReturn(3000);
        writePair("a", 1000);
        writePair("b", 1000);
        StreamsSnapshotReader reader = new StreamsSnapshotReader(getDefaultRuntime(), config);
        reader.setSnapshotBatchSizeHint(Integer.MAX_VALUE);
        assertEquals(2, read(reader).size());
    }
}
