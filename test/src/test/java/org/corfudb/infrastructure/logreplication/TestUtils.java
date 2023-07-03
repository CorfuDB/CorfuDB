package org.corfudb.infrastructure.logreplication;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.runtime.LogReplication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

/**
 * This class contains the common helper functions which can be used by multiple Log Replication unit tests.
 */
public class TestUtils {

    /**
     * Create a LogEntry message with opaque entries starting from startTs to endTs.  Each opaque entry corresponds to a
     * transaction.  The metadata of the message is populated as per the input arguments.
     */
    LogReplication.LogReplicationEntryMsg generateLogEntryMsg(long startTs, long endTs, long prevTs, long topologyConfigId,
                                                              long snapshotTs) {
        List<OpaqueEntry> opaqueEntryList = new ArrayList<>();

        for (long i = startTs; i <= endTs; i++) {
            OpaqueEntry opaqueEntry = new OpaqueEntry(i, Collections.EMPTY_MAP);
            opaqueEntryList.add(opaqueEntry);
        }

        // Create sample metadata
        LogReplication.LogReplicationEntryMetadataMsg metadata = LogReplication.LogReplicationEntryMetadataMsg.newBuilder()
            .setEntryType(LogReplication.LogReplicationEntryType.LOG_ENTRY_MESSAGE)
            .setTimestamp(endTs)
            .setSnapshotTimestamp(snapshotTs)
            .setPreviousTimestamp(prevTs)
            .setTopologyConfigID(topologyConfigId)
            .setSnapshotSyncSeqNum(0)
            .build();

        // Generate the protobuf message to be given to LogEntryWriter
        LogReplication.LogReplicationEntryMsg lrEntryMsg = CorfuProtocolLogReplication.getLrEntryMsg(unsafeWrap(
            CorfuProtocolLogReplication.generatePayload(opaqueEntryList)), metadata);

        return lrEntryMsg;
    }
}
