package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;

import java.util.ArrayList;
import java.util.List;

/**
 * The sliding window to record the pending entries that have sent to the receiver but hasn't got an ACK yet.
 * The alternative is to remember the address only and reset the stream head to rereading the data if the queue size
 * is quite big.
 */
@Slf4j
public class SenderPendingMessageQueue {

    /*
     * The max number of the entries that the queue can contain
     */
    private int maxSize;

    /*
     * The list of pending entries.
     */
    @Getter
    private List<LogReplicationPendingEntry> pendingEntries;


    public SenderPendingMessageQueue(int maxSize) {
        this.maxSize = maxSize;
        this.pendingEntries = new ArrayList<>();
    }

    /**
     * The current number of pending entries
     * @return
     */
    public int getSize() {
        return pendingEntries.size();
    }

    public boolean isEmpty() {
        return pendingEntries.isEmpty();
    }

    public boolean isFull() {
        return pendingEntries.size() >= maxSize;
    }

    public void clear() {
        pendingEntries = new ArrayList<>();
    }

    /**
     * Remove all the entries whose timestamp is not larger than the given ts
     * @param ts
     */
    void evictAccordingToTimestamp(long ts) {
        log.trace("Evict all messages whose timestamp is smaller or equal to {}", ts);

        //As the entries are in the order of timestamp value, we can just remove the first each time
        //until the condition is not met anymore.
        while(!pendingEntries.isEmpty() && pendingEntries.get(0).getData().getMetadata().getTimestamp() <= ts) {
            pendingEntries.remove(0);
        }
    }

    /**
     * Remove all the entries whose snapshotSeqNum is not larger than the given seqNum
     * @param seqNum
     */
    void evictAccordingToSeqNum(long seqNum) {
        log.trace("Evict all messages whose snapshotSeqNum is smaller or equal to {}", seqNum);

        //As the entries are in the order of timestamp value, we can just remove the first each time
        //until the condition is not met anymore.
        while(!pendingEntries.isEmpty() && pendingEntries.get(0).getData().getMetadata().getSnapshotSyncSeqNum() <= seqNum) {
            pendingEntries.remove(0);
        }
    }

    /**
     * Append message to the list according in the order of sending
     *
     * @param data
     */
    void append(LogReplicationEntryMsg data) {
        LogReplicationPendingEntry entry = new LogReplicationPendingEntry(data);
        pendingEntries.add(entry);
    }
}
