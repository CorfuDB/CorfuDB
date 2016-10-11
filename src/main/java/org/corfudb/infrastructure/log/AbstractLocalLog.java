package org.corfudb.infrastructure.log;


import com.google.common.collect.RangeSet;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A LocalLog is the basic building unit of the distributed log. It provides an address space to write to.
 * Log units consume and operate on LocalLogs to construct the distributed log. The LocalLog can be
 * backed by different implementations.
 *
 * Todo(Maithem): Should add LocalLog sync methods
 *
 * Created by maithem on 7/15/16.
 */

public abstract class AbstractLocalLog {
    private final LogRange range;

    protected final String logPathDir;

    protected final boolean sync;

    public AbstractLocalLog(long start, long end, String dir, boolean sync) {
        range = new LogRange(start, end);
        logPathDir = dir;
        this.sync = sync;
        initializeLog();
    }

    public LogRange getRange() {
        return range;
    }

    public boolean inRange(long address) {
        return address >= range.start && address <= range.end;
    }

    private void checkRange(long address) {
        if (!inRange(address)) {
            throw new RuntimeException();
        }
    }

    public void write(long address, LogData entry) {
        checkRange(address);
        backendWrite(address, entry);
    }

    public LogData read(long address) {
        checkRange(address);
        return backendRead(address);
    }


    AtomicLong counter = new AtomicLong();

    // In this default implementation, the counter is in-memory only.
    public Long getToken(int numTokens) {
        return counter.getAndAdd(numTokens);
    }

    public void streamWrite(UUID streamID, RangeSet<Long> entry) {
        backendStreamWrite(streamID, entry);
    }

    public RangeSet<Long> streamRead(UUID streamID) {
        return backendStreamRead(streamID);
    }

    protected abstract void backendWrite(long address, LogData entry);

    protected abstract LogData backendRead(long address);

    protected abstract void backendStreamWrite(UUID streamID, RangeSet<Long> entry);

    protected abstract RangeSet<Long> backendStreamRead(UUID streamID);

    protected abstract void initializeLog();


}
