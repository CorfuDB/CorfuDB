package org.corfudb.infrastructure.logreplication.replication.fsm;

import com.google.protobuf.ByteString;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryMsg;

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

    /*
     * Apart from the Test Data, 5 metadata transactions also take place, namely -
     * 1. 2 TXs for creating the metadata and replication status tables + 1 registry table TX
     * 2. 1 TX for initializing the metadata in metadata table
     * 3. 1 TX for initializing the replication status on Source in the replication status table
     * So the log tail will be num data writes + 5
     */
    private int offset = 5;

    private CorfuRuntime runtime;

    private final long baseSnapshot;

    private List<Long> seqNumsToRead;

    public TestSnapshotReader(TestReaderConfiguration config) {
        this.config = config;
        this.baseSnapshot = config.getNumEntries() + offset;
        this.runtime = new CorfuRuntime(config.getEndpoint()).connect();
    }

    @Override
    public SnapshotReadMessage read(UUID snapshotRequestId) {
        // Connect to endpoint
        List<LogReplicationEntryMsg> messages = new ArrayList<>();

        for (long i : seqNumsToRead) {
            Object data = runtime.getAddressSpaceView().read(i).getPayload(runtime);
            LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                    .setEntryType(LogReplicationEntryType.SNAPSHOT_MESSAGE)
                    .setTopologyConfigID(topologyConfigId)
                    .setTimestamp(i)
                    .setSnapshotTimestamp(baseSnapshot)
                    .setSyncRequestId(getUuidMsg(snapshotRequestId))
                    .build();

            messages.add(getLrEntryMsg(ByteString.copyFrom((byte[]) data), metadata));
            globalIndex++;
        }
        return new SnapshotReadMessage(messages, (globalIndex + offset) == baseSnapshot);
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

    public void setSeqNumsToRead(List<Long>seqNums) {
        this.seqNumsToRead = seqNums;
    }
}
