package org.corfudb.logreplication.receiver;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageMetadata;
import org.corfudb.logreplication.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.transmitter.TxMessage;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@NotThreadSafe
@Slf4j
public class LogEntryWriter {
    private Set<String> streams;
    private List<UUID> streamUUIDs;
    HashMap<UUID, IStreamView> streamViewMap;
    CorfuRuntime rt;
    private long srcGlobalSnapshot;
    long lastMsgTs;
    private final int MAX_MSG_QUE_SIZE = 20;
    private HashMap<Long, TxMessage> msgQ; //If the received messages are out of order, buffer them. Can be queried according to the preTs.

    LogEntryWriter(CorfuRuntime rt, LogReplicationConfig config) {
        this.rt = rt;
        this.streams = config.getStreamsToReplicate();
        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }
        msgQ = new HashMap<>();
        srcGlobalSnapshot = Address.NON_ADDRESS;
        lastMsgTs = Address.NON_ADDRESS;
    }

    /**
     *
     * @param metadata
     * @throws Exception
     */
    void verifyMetadata(MessageMetadata metadata) throws Exception {
        if (metadata.getMessageMetadataType() != MessageType.LOG_ENTRY_MESSAGE || metadata.getSnapshotTimestamp() != srcGlobalSnapshot) {
            log.error("Wrong message metadata {}, expecting  type {} snapshot {}", metadata,
                    MessageType.LOG_ENTRY_MESSAGE, srcGlobalSnapshot);
            throw new Exception("wrong type of message");
        }
    }

    /**
     * Convert message data to an MultiObjectSMREntry and write to log.
     * @param txMessage
     */
    void processMsg(TxMessage txMessage) {
        OpaqueEntry opaqueEntry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(txMessage.getData()));
        Map<UUID, List<SMREntry>> map = opaqueEntry.getEntries();
        if (!streamUUIDs.contains(map.keySet())) {
            log.error("txMessage contains noisy streams {}, expecting {}", map.keySet(), streamUUIDs);
        }
        try {
            rt.getObjectsView().TXBegin();
            TokenResponse tokenResponse = rt.getSequencerView().next((UUID[])(map.keySet().toArray()));
            MultiObjectSMREntry multiObjectSMREntry = new MultiObjectSMREntry();
            for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                for(SMREntry smrEntry : opaqueEntry.getEntries().get(uuid)) {
                    multiObjectSMREntry.addTo(uuid, smrEntry);
                }
            }
            rt.getAddressSpaceView().write(tokenResponse.getToken(), multiObjectSMREntry);

        } finally {
            rt.getObjectsView().TXEnd();
        }
    }

    /**
     * Go over the queue, if the next expecting msg is in queue, process it.
     * @throws Exception
     */
    void processQue() throws Exception {
        while (true) {
            TxMessage txMessage = msgQ.get(lastMsgTs);
            if (txMessage == null) {
                return;
            }
            processMsg(txMessage);
            msgQ.remove(lastMsgTs);
            lastMsgTs = txMessage.getMetadata().getEntryTimeStamp();
        }
    }

    void applyTxMessage(TxMessage msg) throws Exception {
        verifyMetadata(msg.getMetadata());

        // Ignore the out of date messages
        if (msg.getMetadata().getSnapshotTimestamp() < srcGlobalSnapshot) {
            log.warn("Received message with snapshot {} is smaller than current snapshot {}.Ignore it",
                    msg.getMetadata().getSnapshotTimestamp(), srcGlobalSnapshot);
            return;
        }

        // A new full sync happens, setup the new srcGlobalSnapshot
        if (msg.getMetadata().getSnapshotTimestamp() > srcGlobalSnapshot) {
            srcGlobalSnapshot = msg.getMetadata().getSnapshotTimestamp();
            lastMsgTs = srcGlobalSnapshot;
        }

        //we will skip the entries has been processed.
        if (msg.getMetadata().getEntryTimeStamp() <= lastMsgTs) {
            return;
        }

        //If the entry is the expecting entry, process it and then process
        //the messages in the queue.
        if (msg.getMetadata().getPreviousEntryTimestamp() == lastMsgTs) {
            processMsg(msg);
            lastMsgTs = msg.getMetadata().getEntryTimeStamp();
            processQue();
        }

        //If the entry's ts is larger than the entry processed, put it in queue
        if (msgQ.size() < MAX_MSG_QUE_SIZE) {
            msgQ.putIfAbsent(msg.getMetadata().getPreviousEntryTimestamp(), msg);
        }
    }
}
