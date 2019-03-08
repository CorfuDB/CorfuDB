package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamAddressSpace;
import org.corfudb.runtime.view.Address;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * A container object that holds tail offsets, stream addresses and the global
 * log tail that has been seen. Note that holes don't belong to any
 * stream therefore the globalTail needs to be tracked separately.
 *
 * <p>Created by maithem on 10/15/18.
 */

@NotThreadSafe
@ToString
@Slf4j
public class LogMetadata {

    @Getter
    private volatile long globalTail;

    @Getter
    private final Map<UUID, StreamAddressSpace> streamsAddressSpaceMap;

    @Getter
    private final Map<UUID, Long> streamTails;

    public LogMetadata() {
        this.globalTail = Address.NON_ADDRESS;
        this.streamTails = new HashMap();
        this.streamsAddressSpaceMap = new HashMap();
    }

    public void update(List<LogData> entries) {
        for (LogData entry : entries) {
            update(entry);
        }
    }

    public void update(LogData entry) {
        long entryAddress = entry.getGlobalAddress();
        updateGlobalTail(entryAddress);
        for (UUID streamId : entry.getStreams()) {
            // Update stream tails
            long currentStreamTail = streamTails.getOrDefault(streamId, Address.NON_ADDRESS);
            streamTails.put(streamId, Math.max(currentStreamTail, entryAddress));

            // Update stream address map
            // Note: this is used on sequencer recovery
            if (streamsAddressSpaceMap.containsKey(streamId)) {
                streamsAddressSpaceMap.get(streamId).addAddress(entryAddress);
            } else {
                streamsAddressSpaceMap.put(streamId,
                        new StreamAddressSpace(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf(entryAddress)));
            }
        }

        // We should also consider checkpoint metadata while updating the tails.
        // This is important because there could be streams that have checkpoint
        // data on the checkpoint stream, but not entries on the regular stream.
        // If those streams are not updated, then clients would observe those
        // streams as empty, which is not correct.
        if (entry.hasCheckpointMetadata()) {
            UUID streamId = entry.getCheckpointedStreamId();
            long streamTailAtCP = entry.getCheckpointedStreamStartLogAddress();

            if (Address.isAddress(streamTailAtCP)) {
                // TODO(Maithem) This is needed to filter out checkpoints of empty streams,
                // if the map has an entry (streamId, Address.Non_ADDRESS), then
                // when the sequencer services queries on that stream it will
                // "think" that the tail is not empty and return Address.Non_ADDRESS
                // instead of NON_EXIST. The sequencer, should handle both cases,
                // but that can be addressed in another issue.
                long currentStreamTail = streamTails.getOrDefault(streamId, Address.NON_ADDRESS);
                streamTails.put(streamId, Math.max(currentStreamTail, streamTailAtCP));

                // The trim mark is part of the address space information and is also required so clients can observe
                // updates to streams that have been completely checkpointed.
                if (streamsAddressSpaceMap.containsKey(streamId) &&
                        streamsAddressSpaceMap.get(streamId).getAddressCount() != 0) {
                    // Updates to regular stream are present, update trim mark
                    StreamAddressSpace streamAddressSpace = streamsAddressSpaceMap.get(streamId);
                    streamAddressSpace.setTrimMark(Math.max(streamAddressSpace.getTrimMark(), streamTailAtCP));
                } else {
                    // Empty RoaringMap (then update global tail from trim mark at sequencer)
                    streamsAddressSpaceMap.put(streamId,
                            new StreamAddressSpace(streamTailAtCP, new Roaring64NavigableMap()));
                }
            }
        }
    }

    public void updateGlobalTail(long newTail) {
        globalTail = Math.max(globalTail, newTail);
    }

}
