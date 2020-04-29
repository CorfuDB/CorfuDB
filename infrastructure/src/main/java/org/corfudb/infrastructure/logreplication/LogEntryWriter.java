package org.corfudb.infrastructure.logreplication;

import io.netty.buffer.Unpooled;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


/**
 * Process TxMessage that contains transaction logs for registered streams.
 */
@NotThreadSafe
@Slf4j
public class LogEntryWriter {
    private final static int MAX_NUM_TX_RETRY = 3;

    @Setter
    private int maxMsgQueSize = SinkManager.DEFAULT_READER_QUEUE_SIZE; //The max size of the msgQ.

    private Set<UUID> streamUUIDs; //the set of streams that log entry writer will work on.
    HashMap<UUID, IStreamView> streamViewMap; //map the stream uuid to the stream view.
    CorfuRuntime rt;
    private long srcGlobalSnapshot; //the source snapshot that the transaction logs are based
    private long lastMsgTs; //the timestamp of the last message processed.
    private PersistedWriterMetadata persistedWriterMetadata;

    public LogEntryWriter(CorfuRuntime rt, LogReplicationConfig config, PersistedWriterMetadata persistedWriterMetadata) {
        this.rt = rt;
        this.persistedWriterMetadata = persistedWriterMetadata;

        Set<String> streams = config.getStreamsToReplicate();
        streamUUIDs = new HashSet<>();

        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }

        srcGlobalSnapshot = Address.NON_ADDRESS;
        lastMsgTs = Address.NON_ADDRESS;

        streamViewMap = new HashMap<>();

        for (UUID uuid : streamUUIDs) {
            streamViewMap.put(uuid, rt.getStreamsView().getUnsafe(uuid));
        }
    }


    /**
     * Verify the metadata is the correct data type.
     * @param metadata
     * @throws ReplicationWriterException
     */
    void verifyMetadata(LogReplicationEntryMetadata metadata) throws ReplicationWriterException {
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
    void processMsg(LogReplicationEntry txMessage) {
        boolean doRetry = true;
        int numRetry = 0;
        OpaqueEntry opaqueEntry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(txMessage.getPayload()));
        Map<UUID, List<SMREntry>> map = opaqueEntry.getEntries();

        if (!streamUUIDs.containsAll(map.keySet())) {
            log.error("txMessage contains noisy streams {}, expecting {}", map.keySet(), streamUUIDs);
            throw new ReplicationWriterException("Wrong streams set");
        }


        long epoch = txMessage.getMetadata().getSiteEpoch();
        long msgTs = txMessage.getMetadata().timestamp;
        long persistTs = Address.NON_ADDRESS;

        while (doRetry && numRetry++ < MAX_NUM_TX_RETRY) {
            try {
                rt.getObjectsView().TXBegin();
                persistTs = persistedWriterMetadata.getLastProcessedLogTimestamp();
                if (epoch == persistedWriterMetadata.getSiteEpoch() && msgTs > persistTs ) {
                    for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                        for (SMREntry smrEntry : opaqueEntry.getEntries().get(uuid)) {
                            TransactionalContext.getCurrentContext().logUpdate(uuid, smrEntry);
                        }
                    }

                    persistedWriterMetadata.setSiteEpoch(epoch);
                    persistedWriterMetadata.setLastProcessedLogTimestamp(msgTs);
                    log.trace("Will append msg {} as its timestamp is not later than the persisted one {}",
                            txMessage.getMetadata(), persistTs);
                } else {
                    log.warn("Skip write this msg {} as its timestamp is later than the persisted one {}",
                            txMessage.getMetadata(), persistTs);
                }
                doRetry = false;
            } catch (TransactionAbortedException e) {
                log.warn("Caught an exception {} , will retry", e);
            } finally {
                rt.getObjectsView().TXEnd();
            }
        }
        lastMsgTs = Math.max(persistTs, msgTs);
    }

    /**
     * Apply message generate by log entry reader and will apply at the destination corfu cluster.
     * @param msg
     * @return long: the last processed message timestamp if apply processing any messages.
     * @throws ReplicationWriterException
     */
    public long apply(LogReplicationEntry msg) throws ReplicationWriterException {

        verifyMetadata(msg.getMetadata());

        // Ignore the out of date messages
        if (msg.getMetadata().getSnapshotTimestamp() < srcGlobalSnapshot) {
            log.warn("Received message with snapshot {} is smaller than current snapshot {}.Ignore it",
                    msg.getMetadata().getSnapshotTimestamp(), srcGlobalSnapshot);
            return Address.NON_ADDRESS;
        }

        // A new Delta sync is triggered, setup the new srcGlobalSnapshot and msgQ
        if (msg.getMetadata().getSnapshotTimestamp() > srcGlobalSnapshot) {
            srcGlobalSnapshot = msg.getMetadata().getSnapshotTimestamp();
            lastMsgTs = srcGlobalSnapshot;
        }

        // we will skip the entries has been processed.
        if (msg.getMetadata().getTimestamp() <= lastMsgTs) {
            log.warn("Received message with snapshot {} is smaller than lastMsgTs {}.Ignore it",
                    msg.getMetadata().getSnapshotTimestamp(), lastMsgTs);
            return Address.NON_ADDRESS;
        }

        //If the entry is the expecting entry, process it and process
        //the messages in the queue.
        if (msg.getMetadata().getPreviousTimestamp() == lastMsgTs) {
            processMsg(msg);
            return lastMsgTs;
        }

        return Address.NON_ADDRESS;
    }

    /**
     * Set the base snapshot that last full sync based on and ackTimesstamp
     * that is the last log entry it has played.
     * This is called while the writer enter the log entry sync state.
     *
     * @param snapshot
     * @param ackTimestamp
     */
    public void setTimestamp(long snapshot, long ackTimestamp) {
        srcGlobalSnapshot = snapshot;
        lastMsgTs = ackTimestamp;
    }
}
