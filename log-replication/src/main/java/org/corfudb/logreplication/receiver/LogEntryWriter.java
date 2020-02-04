package org.corfudb.logreplication.receiver;

import org.corfudb.logreplication.MessageMetadata;
import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.logreplication.transmitter.TxMessage;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.Comparator;
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
    private long srcGlobalSnapshot;
    private long lastSrcAddressProcessed;
    private PriorityQueue<TxMessage> msgQ;

    LogEntryWriter(LogReplicationContext context) {
    }

    void verifyMetadata(MessageMetadata metadata) {

    }

    void processTxMessage(TxMessage msg) {
        MessageMetadata metadata = msg.getMetadata();
        verifyMetadata(metadata);
        TxMessage currentMsg = null;

        assert(metadata.getPreviousEntryTimestamp() == proccessedMsgTs);
        for (UUID streamID : streamUUIDs) {
            ILogData logData = null;
            MultiObjectSMREntry multiObjSMREntry = (MultiObjectSMREntry)logData.getPayload(rt);
            for (SMREntry entry : multiObjSMREntry.getSMRUpdates(streamID)) {
                streamViewMap.get(streamID).append(entry);
            }
        }
    }

    /**
     * find the msg whose
     * @param preAddress
     * @return
     */
    TxMessage findMsg(long preAddress) {
        //delete all message whose address < preAddress
        //return the msg whose preAddress == preAddress
        return new TxMessage();
    }

    void setContext(long snapshot) {
        srcGlobalSnapshot = snapshot;
        lastSrcAddressProcessed = snapshot;
    }

    //Question? what the size of the deltaQue? Is it a in memory que or persistent que?
    void processDeltaQue() {
        while (true) {
            TxMessage msg = findMsg(lastSrcAddressProcessed);
            if (msg == null) {
                return;
            }

            processTxMessage(msg);
            lastSrcAddressProcessed = msg.getMetadata().getEntryTimeStamp();
            //remove msg from deltaQue
        }
    }
}
