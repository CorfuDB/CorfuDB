package org.corfudb.runtime.collections.streaming;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.view.AddressSpaceView;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * This class represents a composite {@link DeltaStream} which tracks multiple streams(stream tags) in a single ordered
 * buffer.  The buffer is constructed from the unified address space of all streams being tracked.  The consumption
 * and read apis have the same behavior as its super class - DeltaStream.
 *
 * LRDeltaStream is exclusively used by external applications of Log Replication(LR) to receive ordered streaming
 * updates across the System Table(LogReplicationStatus) and application tables.
 */
@Slf4j
public class LRDeltaStream extends DeltaStream {

    @Getter
    private final List<UUID> streamsTracked;

    public LRDeltaStream(AddressSpaceView addressSpaceView, UUID streamId, long lastAddressRead, int bufferSize,
                         List<UUID> streamsTracked) {
        super(addressSpaceView, streamId, lastAddressRead, bufferSize);
        this.streamsTracked = streamsTracked;
    }

    @Override
    protected void validateBackpointerPresence(ILogData logData) {
        Set<UUID> streamsWithBackpointer = logData.getBackpointerMap().keySet();
        for (UUID stream : streamsTracked) {
            if (streamsWithBackpointer.contains(stream)) {
                return;
            }
        }
        throw new IllegalStateException(String.format("%s does not contain a backpointer to any stream being tracked %s",
                Arrays.toString(streamsWithBackpointer.toArray()), Arrays.toString(streamsTracked.toArray())));
    }
}
