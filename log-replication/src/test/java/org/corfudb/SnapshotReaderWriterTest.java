package org.corfudb;

import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.transmitter.DataMessage;
import org.corfudb.logreplication.transmitter.DefaultReadProcessor;
import org.corfudb.logreplication.transmitter.SnapshotReadMessage;
import org.corfudb.logreplication.transmitter.StreamsSnapshotReader;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class SnapshotReaderWriterTest {

    //Generate data and the same time push the data to the hashtable
    void generateData(Set<String> streams) {

    }

    void ckStreams(Set<String> streams) {

    }

    void readMsgs(List<DataMessage> msgQ, Set<String > streams, CorfuRuntime rtTx) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        DefaultReadProcessor readProcessor = new DefaultReadProcessor(rtTx);
        StreamsSnapshotReader reader = new StreamsSnapshotReader(rtTx, config, readProcessor);

        while (true) {
            SnapshotReadMessage snapshotReadMessage = reader.read();
            msgQ.addAll(snapshotReadMessage.getMessages());
            if (snapshotReadMessage.isEndRead()) {
                break;
            }
        }
    }

    void test0() {
        //define transmitter runtime and streams to replicate
        CorfuRuntime rtTx = null;
        Set<String> streams = new HashSet<>();
        streams.add("test0");
        streams.add("test1");
        List<DataMessage> msgQ = new ArrayList<>();

        //generate some data in the streams
        //including a checkpoint in the streams
        generateData(streams);
        ckStreams(streams);
        readMsgs(msgQ, streams,rtTx);

        //define receiver runtime and streams to write to
        //play msgQ with snapshot writer
    }
}
