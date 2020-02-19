package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.message.LogReplicationEntry;
import org.corfudb.logreplication.message.LogReplicationEntryMetadata;
import org.corfudb.logreplication.message.MessageType;
import org.corfudb.logreplication.send.SnapshotReadMessage;
import org.corfudb.logreplication.send.SnapshotReader;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Dummy implementation of snapshot reader for testing purposes.
 *
 * This reader attempts to access n entries in the log in a continuous address space and
 * wraps the payload in the DataMessage.
 */
public class TestSnapshotReader implements SnapshotReader {

    private TestReaderConfiguration config;

    private int globalIndex = 0;

    private CorfuRuntime runtime;

    public TestSnapshotReader(TestReaderConfiguration config) {
        this.config = config;
        this.runtime = new CorfuRuntime(config.getEndpoint()).connect();
    }

    @Override
    public SnapshotReadMessage read() {
        // Connect to endpoint
        List<LogReplicationEntry> messages = new ArrayList<>();

        int index = globalIndex;

        // Read numEntries in consecutive address space and add to messages to return
        for (int i=index; (i<(index+config.getBatchSize()) && index<config.getNumEntries()) ; i++) {
            Object data = runtime.getAddressSpaceView().read((long)i).getPayload(runtime);
            // For testing we don't have access to the snapshotSyncId so we fill in with a random UUID
            // and overwrite it in the TestDataSender with the correct one, before sending the message out
            LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_MESSAGE,
                    i, config.getNumEntries(), UUID.randomUUID());
            messages.add(new LogReplicationEntry(metadata, (byte[])data));
            globalIndex++;
        }

        return new SnapshotReadMessage(messages, globalIndex == config.getNumEntries());
    }

    @Override
    public void reset(long snapshotTimestamp) {
        globalIndex = 0;
    }

    public void setBatchSize(int batchSize) {
        config.setBatchSize(batchSize);
    }
}
