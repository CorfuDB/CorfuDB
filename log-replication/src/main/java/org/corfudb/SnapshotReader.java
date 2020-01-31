package org.corfudb;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.StreamViewSMRAdapter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;


/**
 * Read streams one by one, get all the entries.
 */
public class SnapshotReader {
    private final int MAX_BATCH_SIZE = 1000;
    private final MessageType MSG_TYPE = MessageType.SNAPSHOT_MESSAGE;
    private long globalSnapshot;
    private Set<UUID> streams;
    private LinkedList<UUID> streamsToSend;
    private CorfuRuntime rt;

    /**
     * Set runtime and gobalSnapshot
     * @param runtime
     */
    SnapshotReader(CorfuRuntime runtime) {
        rt = runtime;
    }

    void init() {
        globalSnapshot = rt.getAddressSpaceView().getLogTail();
        streamsToSend = new LinkedList<>(streams);
    }

    /**
     * get all entries upto globalSnapshot for an given stream
     * @param streamID
     * @return
     */
    List<SMREntry> readStream(UUID streamID) {
        StreamViewSMRAdapter smrAdapter =  new StreamViewSMRAdapter(rt, rt.getStreamsView().getUnsafe(streamID));
        return smrAdapter.remainingUpTo(globalSnapshot);
    }

    TxMessage generateMessage(List<SMREntry> entries) {
        TxMessage txMessage = new TxMessage();
        //set metadata
        //set data
        return  txMessage;
    }

    List<TxMessage> nextMsgs() {
        List<TxMessage> msgList = new ArrayList<>();
        //get the firstStream to process
        ArrayList<SMREntry> entries = new ArrayList<>(readStream(streamsToSend.poll()));
        for (int i = 0; i < entries.size(); i += MAX_BATCH_SIZE) {
            List<SMREntry> msg_entries = entries.subList(i, i + MAX_BATCH_SIZE);
            TxMessage txMsg = generateMessage(msg_entries);
            msgList.add(txMsg);
        }
        return msgList;
    }

    /**
     * while sync finish put an event to the queue
     */
    void sync() {
        init();
        try {
            while (!streamsToSend.isEmpty()) {
                List<TxMessage> msgs = nextMsgs();
                //call back handler
                //handle_msg(msgs);
            }
        } catch (Exception e) {
            //handle exception
        }
    }
}
