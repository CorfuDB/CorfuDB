package org.corfudb.logreplication.receiver;

import org.corfudb.logreplication.MessageMetadata;
import org.corfudb.logreplication.transmitter.TxMessage;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.UUID;

/**
 * The AR will pass in FullSyncQue and DeltaQue and API for fullSyncDone()
 * Open streams interested and append all entries
 */

public class StreamsSnapshotWriter implements SnapshotWriter {
    private List<UUID> streamUUIDs;
    HashMap<UUID, IStreamView> streamViewMap;
    CorfuRuntime rt;
    long proccessedMsgTs;
    final private int QUEUE_SIZE = 20;
    private PriorityQueue<TxMessage> msgQ;
    private long srcGlobalSnapshot;

    StreamsSnapshotWriter() {
        //init rt, streamUUIDs, srcGlobalSnapshot
        msgQ = new PriorityQueue(QUEUE_SIZE, Comparator.comparingLong(a ->(((TxMessage)a).metadata.entryTimeStamp)));
    }

    /**
     * clear all tables interested
     */
    void clearTables() {
        for (UUID stream : streamUUIDs) {
            CorfuTable<String, String> corfuTable = rt.getObjectsView()
                    .build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setStreamID(stream)
                    .open();
            corfuTable.clear();
            corfuTable.close();
        }
    }

    /**
     * open all streams interested
     */
    void openStreams() {
        for (UUID streamID : streamUUIDs) {
            IStreamView sv = rt.getStreamsView().getUnsafe(streamID);
            streamViewMap.put(streamID, sv);
        }
    }

    void processSMREntries(UUID streamId, List<SMREntry> entries) {
        for (SMREntry entry : entries) {
            streamViewMap.get(streamId).append(entry);
        }
    }

    /**
     * if the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     * @return
     */
    void verifyMetadata(MessageMetadata metadata) {
    }

    // If the message is out of order, buffer it. If buffer is overflown, thrown an exception.
    // Will define the exception type later.
    // will use Maithem's api to query the msg to get the uuid and
    // list of smr entries
    void processTxMessage(TxMessage msg) throws Exception {
        MessageMetadata metadata = msg.getMetadata();
        verifyMetadata(metadata);
        TxMessage currentMsg = null;

        //decide to queue message or not according the snapshot value
        if (metadata.getPreviousEntryTimestamp() > proccessedMsgTs) {
            msgQ.add(msg);
            TxMessage first = msgQ.peek();
            if (first.getMetadata().getPreviousEntryTimestamp() == proccessedMsgTs) {
                currentMsg = msgQ.poll();
            }
        } else if (metadata.getPreviousEntryTimestamp() == proccessedMsgTs){
            currentMsg = msg;
        }

        if (currentMsg != null) {
            // process the message
            //UUID streamID = streamUUIDs.get(0);
            //List<SMREntry> entries; //get the entries from the msg
            //processSMREntries(streamID, entries);
        }
    }

    /**
     *
     */
    void reset(long snapshot) {
       srcGlobalSnapshot = snapshot;
       msgQ.clear();
    }

    /**
     * The fullSyncQue guarantee the ordering of the messages.
     */
    void processFullSyncQue() {
        //reset();
        //get message, call processTxMessage()
    }

    @Override
    public void apply(TxMessage message) {

    }

    @Override
    public void apply(List<TxMessage> messages) {

    }
}
