package org.corfudb.logreplication.receiver;

import org.corfudb.logreplication.MessageMetadata;
import org.corfudb.logreplication.transmitter.TxMessage;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.UUID;

public class LogEntryWriter {
    private List<UUID> streamUUIDs;
    HashMap<UUID, IStreamView> streamViewMap;
    CorfuRuntime rt;
    long proccessedMsgTs;
    final private int QUEUE_SIZE = 20;
    private PriorityQueue<TxMessage> msgQ;
    private long srcGlobalSnapshot;
    private long lastSrcAddressProcessed;

    // As the DeltaQueue doesn't guarantee the ordering, need buffering.
    void processTxMessage(TxMessage msg) throws Exception {
        MessageMetadata metadata = msg.getMetadata();
        //verifyMetadata(metadata);
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
            //update proccessedMsgTs and also last srcAddressProcessed
        }
    }
}
