package org.corfudb.logreplication.send;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;

/**
 * The sliding window to record the pending entries that have sent to the receiver but hasn't got an ACK yet.
 * The alternative is to remember the address only and reset the stream head to rereading the data if the queue size
 * is quite big.
 */
@Slf4j
public class LogReplicationSenderQueue {

    // The max number of the entries that the queue can contain
    private int maxSize;

    @Getter
    private ArrayList<LogReplicationPendingEntry> list;

    /*
     * reset while process messages
     */
    long currentTime;


    public LogReplicationSenderQueue(int maxSize) {
        this.maxSize = maxSize;
        list = new ArrayList<>();
    }

    public int getSize() {
        return list.size();
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    public boolean isFull() {
        return (list.size() == maxSize);
    }

    public void evictAll() {
        list = new ArrayList<>();
    }

    void evictAll(long address) {
        log.trace("evict address " + address);
        list.removeIf(a -> (a.data.getMetadata().getTimestamp() <= address));
    }

    void append(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry data, long timer) {
        LogReplicationPendingEntry entry = new LogReplicationPendingEntry(data, timer);
        list.add(entry);
    }


}
