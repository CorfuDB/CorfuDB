package org.corfudb.infrastructure.logreplication.replication.fsm;

import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Dummy implementation of snapshot log reader for testing purposes.
 *
 * This log reader attempts to access n entries in the log in a continuous address space and
 * wraps the payload in the LogReplicationEntry.
 */
public class TestSnapshotReader implements SnapshotReader {

    private long topologyConfigId = 0;

    private final int FIRST_ADDRESS = 0;

    private TestReaderConfiguration config;

    // Initialized to 2, as 0, 1 are always used to persist metadata
    private int globalIndex = FIRST_ADDRESS;

    private CorfuRuntime runtime;

    public TestSnapshotReader(TestReaderConfiguration config) {
        this.config = config;
        this.runtime = new CorfuRuntime(config.getEndpoint()).connect();
    }

    @Override
    public SnapshotReadMessage read(UUID snapshotRequestId) {
        // Connect to endpoint
        List<LogReplicationEntry> messages = new ArrayList<>();

        // Read numEntries in consecutive address space and add to messages to return
        for (int i= globalIndex; i < (config.getNumEntries() + FIRST_ADDRESS) ; i++) {
            Object data = runtime.getAddressSpaceView().read((long)i).getPayload(runtime);
            // For testing we don't have access to the snapshotSyncId so we fill in with a random UUID
            // and overwrite it in the TestDataSender with the correct one, before sending the message out
            LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_MESSAGE, topologyConfigId,
                    snapshotRequestId, i, config.getNumEntries(), UUID.randomUUID());
            messages.add(new LogReplicationEntry(metadata, (byte[])data));
            globalIndex++;
        }

        return new SnapshotReadMessage(messages, globalIndex == (config.getNumEntries() + FIRST_ADDRESS));
    }

    @Override
    public void reset(long snapshotTimestamp) {
        globalIndex = FIRST_ADDRESS;
    }

    @Override
    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }

    public void setBatchSize(int batchSize) {
        config.setBatchSize(batchSize);
    }
}
