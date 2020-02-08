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
/**
 * Process TxMessage that contains transaction logs for registered streams.
 */
public class LogEntryWriter {
    private List<UUID> streamUUIDs; //the set of streams that log entry writer will work on.
    HashMap<UUID, IStreamView> streamViewMap; //map the stream uuid to the streamview.
    CorfuRuntime rt;
    private long srcGlobalSnapshot; //the source snapshot that the transaction logs are based
    long lastMsgTs; //the timestamp of the last message processed.
    private HashMap<Long, TxMessage> msgQ; //If the received messages are out of order, buffer them. Can be queried according to the preTs.
    private final int MAX_MSG_QUE_SIZE = 20; //The max size of the msgQ.
    private PersistedWriterMetadata persistedWriterMetadata;

    LogEntryWriter(CorfuRuntime rt, LogReplicationConfig config) {
        this.rt = rt;
        Set<String> streams = config.getStreamsToReplicate();
        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }
        msgQ = new HashMap<>();
        srcGlobalSnapshot = Address.NON_ADDRESS;
        lastMsgTs = Address.NON_ADDRESS;
        persistedWriterMetadata = new PersistedWriterMetadata(rt, config.getSiteID(), config.getRemoteSiteID());
    }

    /**
     * Verify the metadata is the correct data type.
     * @param metadata
     * @throws ReplicationWriterException
     */
    void verifyMetadata(MessageMetadata metadata) throws ReplicationWriterException {
        if (metadata.getMessageMetadataType() != MessageType.LOG_ENTRY_MESSAGE) {
            log.error("Wrong message metadata {}, expecting  type {} snapshot {}", metadata,
                    MessageType.LOG_ENTRY_MESSAGE, srcGlobalSnapshot);
            throw new ReplicationWriterException("wrong type of message");
        }
    }

    /**
     * Convert message data to an MultiObjectSMREntry and write to log.
     * @param txMessage
     */
    void processMsg(TxMessage txMessage) {
        OpaqueEntry opaqueEntry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(txMessage.getData()));
        Map<UUID, List<SMREntry>> map = opaqueEntry.getEntries();
        if (!streamUUIDs.containsAll(map.keySet())) {
            log.error("txMessage contains noisy streams {}, expecting {}", map.keySet(), streamUUIDs);
            throw new ReplicationWriterException("Wrong streams set");
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
        persistedWriterMetadata.setLastProcessedLogTimestamp(txMessage.getMetadata().getEntryTimeStamp());
    }

    /**
     * Go over the queue, if the next expecting msg is in queue, process it.
     */
    void processQueue() {
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

    /**
     * Remove entries that has timestamp smaller than msgTs
     * @param msgTs
     */
    void cleanMsgQ(long msgTs) {
        for (long address : msgQ.keySet()) {
            if (msgQ.get(address).getMetadata().getSnapshotTimestamp() <= lastMsgTs) {
                msgQ.remove(address);
            }
        }
    }

    void applyTxMessage(TxMessage msg) throws ReplicationWriterException {
        verifyMetadata(msg.getMetadata());

        // Ignore the out of date messages
        if (msg.getMetadata().getSnapshotTimestamp() < srcGlobalSnapshot) {
            log.warn("Received message with snapshot {} is smaller than current snapshot {}.Ignore it",
                    msg.getMetadata().getSnapshotTimestamp(), srcGlobalSnapshot);
            return;
        }

        // A new Delta sync is triggered, setup the new srcGlobalSnapshot and msgQ
        if (msg.getMetadata().getSnapshotTimestamp() > srcGlobalSnapshot) {
            srcGlobalSnapshot = msg.getMetadata().getSnapshotTimestamp();
            lastMsgTs = srcGlobalSnapshot;
            cleanMsgQ(lastMsgTs);
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
            processQueue();
        }

        //If the entry's ts is larger than the entry processed, put it in queue
        if (msgQ.size() < MAX_MSG_QUE_SIZE) {
            msgQ.putIfAbsent(msg.getMetadata().getPreviousEntryTimestamp(), msg);
        } else if (msgQ.get(msg.getMetadata().getPreviousEntryTimestamp()) != null) {
            log.warn("The message is out of order and the queue is full, will drop the message {}", msg.getMetadata());
        }
    }
}
