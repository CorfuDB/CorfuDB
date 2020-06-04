package org.corfudb.infrastructure.logreplication.receive;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import java.util.HashMap;

@Slf4j
/**
 * For snapshot sync and log entry sync, it is possible that the messages generated by the primary site will
 * be delivered out of order due to message loss, intermittent network connection loss, or congestion.
 * At the receiver site we keep a buffer to store the out of order messages and apply them at the receiver site in order.
 * For snapshot sync, the message will be applied according the message's snapshotSeqNumber.
 * For log entry sync, each message has a pre pointer that is a timestamp of the previous message, this guarantees that
 * the messages will be applied in order.
 * At the same time, it will send an ACK to notify the sender site the messages has received in order so far.
 */
public abstract class SinkBufferManager {

    /*
     * The buffer is implemented as a hashmap.
     * For logEntry buffer, the key is the entry's previousTimeStamp
     * For Snapshot buffer, the key is the previous entry's snapshotSeqNumber
     */
    HashMap<Long, LogReplicationEntry> buffer;

    /*
     * While processing a message in the buffer, it will call
     * sinkManager to handle it.
     */
    private LogReplicationSinkManager sinkManager;

    /*
     * Could be LOG_ENTRY or SNAPSHOT
     */
    MessageType type;

    /*
     * The max number of entries in the buffer.
     */
    private int maxSize;

    /*
     * How frequent in time, the ack will be sent.
     */
    private int ackCycleTime;

    /*
     * How frequent in number of messages it has received.
     */
    private int ackCycleCnt;

    /*
     * Count the number of messages it has received since last sent ACK.
     */
    private int ackCnt = 0;

    /*
     * Time last ack sent.
     */
    private long ackTime = 0;

    /*
     * The lastProcessedSeq message's ack value.
     * For snapshot, it is the entry's seqNumber.
     * For log entry, it is the entry's timestamp.
     */
    long lastProcessedSeq;

    /**
     *
     * @param type
     * @param ackCycleTime
     * @param ackCycleCnt
     * @param size
     * @param lastProcessedSeq
     * @param sinkManager
     */
    public SinkBufferManager(MessageType type, int ackCycleTime, int ackCycleCnt, int size, long lastProcessedSeq, LogReplicationSinkManager sinkManager) {
        this.type = type;
        this.ackCycleTime = ackCycleTime;
        this.ackCycleCnt = ackCycleCnt;
        this.maxSize = size;
        this.sinkManager = sinkManager;
        this.lastProcessedSeq = lastProcessedSeq;
        buffer = new HashMap<>();
    }

    /**
     * Go through the buffer to find messages that are in order with the last processed message.
     */
    void processBuffer() {
        while (true) {
            LogReplicationEntry dataMessage = buffer.get(lastProcessedSeq);
            if (dataMessage == null)
                return;
            sinkManager.processMessage(dataMessage);
            ackCnt++;
            buffer.remove(lastProcessedSeq);
            lastProcessedSeq = getCurrentSeq(dataMessage);
        }
    }


    /**
     * after receiving a message, it will decide to send an Ack or not
     * according the predefined metrics.
     *
     * @return
     */
    boolean shouldAck() {
        long currentTime = java.lang.System.currentTimeMillis();

        if (ackCnt >= ackCycleCnt || (currentTime - ackTime) >= ackCycleTime) {
            ackCnt = 0;
            ackTime = currentTime;
            return true;
        }

        return false;
    }

    /**
     * If the message is the expected message in order, will skip the buffering and pass to sinkManager to process it;
     * then update the lastProcessedSeq value. If the next expected messages in order are in the buffer,
     * will process all till hitting the missing one.
     *
     * If the message is not the expected message, put the entry into the buffer if there is space.
     * @param dataMessage
     */
    public LogReplicationEntry processMsgAndBuffer(LogReplicationEntry dataMessage) {

        if (verifyMessageType(dataMessage) == false)
           return null;

        long preTs = getPreSeq(dataMessage);
        long currentTs = getCurrentSeq(dataMessage);

        if (preTs == lastProcessedSeq) {
            sinkManager.processMessage(dataMessage);
            ackCnt++;
            lastProcessedSeq = getCurrentSeq(dataMessage);
            processBuffer();
        } else if (currentTs > lastProcessedSeq && buffer.size() < maxSize) {
                buffer.put(preTs, dataMessage);
        }

        System.out.print("\nSink Buffer Send an ACK, lastProcessedSeq " + lastProcessedSeq + " currePreTs " + preTs);

        /*
         * Send Ack with lastProcessedSeq
         */
        if (shouldAck()) {
            LogReplicationEntryMetadata metadata = makeAckMessage(dataMessage);
            return new LogReplicationEntry(metadata, new byte[0]);
        }

        return null;
    }

    /**
     * Get the previous inorder message's sequence.
     * @param entry
     * @return
     */
    abstract long getPreSeq(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry entry);

    /**
     * Get the current message's sequence.
     * @param entry
     * @return
     */
    abstract long getCurrentSeq(LogReplicationEntry entry);

    /**
     * Make an Ack with the lastProcessedSeq
     * @param entry
     * @return
     */
    public abstract LogReplicationEntryMetadata makeAckMessage(LogReplicationEntry entry);

    /*
     * Verify if the message is the correct type.
     * @param entry
     * @return
     */
    public abstract boolean verifyMessageType(LogReplicationEntry entry);
}
