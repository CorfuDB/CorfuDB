package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;

import java.util.ArrayList;

/**
 * The sliding window to record the pending entries that have sent to the receiver but hasn't got an ACK yet.
 * The alternative is to remember the address only and reset the stream head to rereading the data if the queue size
 * is quite big.
 */
@Slf4j
public class LogReplicationPendingEntryQueue {

    /*
     * The max number of the entries that the queue can contain
     */
    private int maxSize;

    /*
     * The list of pending entries.
     */
    @Getter
    private ArrayList<LogReplicationPendingEntry> list;


    public LogReplicationPendingEntryQueue(int maxSize) {
        this.maxSize = maxSize;
        list = new ArrayList<>();
    }

    /**
     * The current number of pending entries
     * @return
     */
    public int getSize() {
        return list.size();
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    public boolean isFull() {
        return (list.size() >= maxSize);
    }

    public void clear() {
        list = new ArrayList<>();
    }

    /**
     * Remove all the entries whose timestamp is not larger than the address
     * @param address
     */
    public void evictAccordingToTimestamp(long address) {
        log.trace("evict address " + address);
        list.removeIf(a -> (a.getData().getMetadata().getTimestamp() <= address));
    }

    public void evictAccordingToSeqNum(long seqNum) {
        log.trace("evict seqNum " + seqNum);
        list.removeIf(a -> (a.getData().getMetadata().getSnapshotSyncSeqNum() <= seqNum));
    }

    public void append(LogReplicationEntryMsg data) {
        LogReplicationPendingEntry entry = new LogReplicationPendingEntry(data);
        list.add(entry);
    }

}
