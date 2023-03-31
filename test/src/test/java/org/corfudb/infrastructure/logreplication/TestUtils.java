package org.corfudb.infrastructure.logreplication;

import com.google.protobuf.Message;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.LogReplication.ReplicationSubscriber;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * This class contains the common helper functions which can be used by multiple Log Replication unit tests.
 */
public class TestUtils {

    private static final String defaultClusterId = UUID.randomUUID().toString();

    /**
     * Create a LogEntry message with opaque entries starting from startTs to endTs.  Each opaque entry corresponds to a
     * transaction.  The metadata of the message is populated as per the input arguments.
     */
    LogReplicationEntryMsg generateLogEntryMsg(long startTs, long endTs, long prevTs, long topologyConfigId,
                                                              long snapshotTs) {
        List<OpaqueEntry> opaqueEntryList = new ArrayList<>();

        for (long i = startTs; i <= endTs; i++) {
            OpaqueEntry opaqueEntry = new OpaqueEntry(i, Collections.EMPTY_MAP);
            opaqueEntryList.add(opaqueEntry);
        }

        // Create sample metadata
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
            .setEntryType(LogReplicationEntryType.LOG_ENTRY_MESSAGE)
            .setTimestamp(endTs)
            .setSnapshotTimestamp(snapshotTs)
            .setPreviousTimestamp(prevTs)
            .setTopologyConfigID(topologyConfigId)
            .setSnapshotSyncSeqNum(0)
            .build();

        // Generate the protobuf message to be given to LogEntryWriter
        LogReplicationEntryMsg lrEntryMsg = CorfuProtocolLogReplication.getLrEntryMsg(unsafeWrap(
            CorfuProtocolLogReplication.generatePayload(opaqueEntryList)), metadata);

        return lrEntryMsg;
    }

    public static  Table<LogReplicationSession, ReplicationStatus, Message> openReplicationStatusTable(
        CorfuStore corfuStore) throws Exception {

        return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_STATUS_TABLE_NAME,
            LogReplicationSession.class, ReplicationStatus.class, null,
            TableOptions.fromProtoSchema(ReplicationStatus.class));
    }

    public static void setSnapshotSyncOngoing(CorfuStore corfuStore,
                                              Table<LogReplicationSession, ReplicationStatus, Message>
                                                  replicationStatusTable, String clientName,
                                              boolean ongoing) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            LogReplicationSession key = getTestSession(clientName);
            ReplicationStatus val = getTestReplicationStatus(ongoing);
            txn.putRecord(replicationStatusTable, key, val, null);
            txn.commit();
        }
    }

    private static LogReplicationSession getTestSession(String clientName) {
        ReplicationSubscriber subscriber = ReplicationSubscriber.newBuilder().setClientName(clientName)
            .setModel(ReplicationModel.LOGICAL_GROUPS)
            .build();
        return LogReplicationSession.newBuilder().setSourceClusterId(defaultClusterId)
            .setSinkClusterId(defaultClusterId).setSubscriber(subscriber)
            .build();
    }

    private static ReplicationStatus getTestReplicationStatus(boolean ongoing) {
        return ReplicationStatus.newBuilder()
            .setSinkStatus(LogReplication.SinkReplicationStatus.newBuilder().setDataConsistent(!ongoing))
            .build();
    }
}
