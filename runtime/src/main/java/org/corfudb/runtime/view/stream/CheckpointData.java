package org.corfudb.runtime.view.stream;

import java.util.UUID;

import org.corfudb.runtime.view.Address;

import lombok.Data;

/**
 * Created by maithem on 7/24/17.
 */
@Data
public class CheckpointData {
    private UUID id = null;
    private long startAddr = Address.NEVER_READ;
    private long endAddr = Address.NEVER_READ;
    private long numEntries;
    private long size;

    /** The address the current checkpoint snapshot was taken at.
     *  The checkpoint guarantees for this stream there are no entries
     *  between checkpointSuccessStartAddr and checkpointSnapshotAddress.
     */
    long snapshotAddress = Address.NEVER_READ;

    public boolean isReady() {
        return (id != null
                && startAddr != Address.NEVER_READ
                && endAddr != Address.NEVER_READ
                && snapshotAddress != Address.NEVER_READ);
    }

    public void unset() {
        startAddr = Address.NEVER_READ;
        endAddr = Address.NEVER_READ;
        numEntries = 0;
        size = 0;
    }

    public static CheckpointData empty = new CheckpointData();
}
