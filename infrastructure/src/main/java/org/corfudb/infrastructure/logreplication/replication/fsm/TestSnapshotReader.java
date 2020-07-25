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
 * Dummy implementation of snapshot reader for testing purposes.
 *
 * This reader attempts to access n entries in the log in a continuous address space and
 * wraps the payload in the LogReplicationEntry.
 */
public class TestSnapshotReader implements SnapshotReader {

    private long topologyConfigId = 0;

    private TestReaderConfiguration config;

    private int globalIndex = 0;

    private CorfuRuntime runtime;

    private final long baseSnapshot;

    public TestSnapshotReader(TestReaderConfiguration config) {
        this.config = config;
        this.baseSnapshot = config.getNumEntries();
        this.runtime = new CorfuRuntime(config.getEndpoint()).connect();
    }

    @Override
    public SnapshotReadMessage read(UUID snapshotRequestId) {
        // Connect to endpoint
        List<LogReplicationEntry> messages = new ArrayList<>();

        int index = globalIndex;

        // Limit to read as max as BatchSize and until the maximum baseSnapshot
        for (int i=index; (i<(index+config.getBatchSize()) && index<baseSnapshot) ; i++) {
        // Read numEntries in consecutive address space and add to messages to return
            Object data = runtime.getAddressSpaceView().read((long)i).getPayload(runtime);
            LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_MESSAGE,
                    topologyConfigId, i, baseSnapshot, snapshotRequestId);
            messages.add(new LogReplicationEntry(metadata, (byte[])data));
            globalIndex++;
        }

        return new SnapshotReadMessage(messages, globalIndex == baseSnapshot);
    }

    @Override
    public void reset(long snapshotTimestamp) {
        globalIndex = 0;
    }

    @Override
    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }

    public void setBatchSize(int batchSize) {
        config.setBatchSize(batchSize);
    }
}
