package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.transmit.SnapshotReadMessage;
import org.corfudb.logreplication.transmit.SnapshotReader;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.List;

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
        List<DataMessage> messages = new ArrayList<>();

        int index = globalIndex;

        // Read numEntries in consecutive address space and add to messages to return
        for (int i=index; (i<(index+config.getBatchSize()) && index<config.getNumEntries()) ; i++) {
            Object data = runtime.getAddressSpaceView().read((long)i).getPayload(runtime);
            messages.add(new DataMessage((byte[])data));
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
