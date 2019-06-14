package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.runtime.view.Address;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A container object that holds log tail offsets and the global
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
        this.streamTails = new HashMap<>();
        this.streamsAddressSpaceMap = new HashMap<>();
    }

    public void update(List<LogData> entries) {
        for (LogData entry : entries) {
            update(entry);
        }
    }

    public void update(LogData entry) {
        update(entry, false, Address.NON_ADDRESS);
    }

    public void update(LogData entry, boolean initialize, long globalTrimMark) {
        long entryAddress = entry.getGlobalAddress();
        updateGlobalTail(entryAddress);
        for (UUID streamId : entry.getStreams()) {
            // Update stream tails
            long currentStreamTail = streamTails.getOrDefault(streamId, Address.NON_ADDRESS);
            streamTails.put(streamId, Math.max(currentStreamTail, entryAddress));

            // Update stream address map (used for sequencer recovery)
            // Since entries might have been written in random order
            // We update the trim mark to be the min of all backpointer addresses.
            streamsAddressSpaceMap.computeIfAbsent(streamId, k ->
                    new StreamAddressSpace(Address.NON_EXIST, new Roaring64NavigableMap()));

            streamsAddressSpaceMap.compute(streamId, (id, addressSpace) -> {
                // If restarting the log unit, i.e., scanning all records in the log for initialization,
                // update the stream trim mark as we read (as entries might not be ordered).
                // Otherwise, i.e., on log updates (writes) we should not consider this data point for setting
                // the stream trim mark, as data might not be written in order, hence, we could have invalid
                // states of the actual address map. In the case of log updates, the trim mark will be set
                // as prefix trims are performed.
                if (addressSpace == null) {
                    long streamTrimMark = Address.NON_EXIST;
                    if (initialize) {
                        streamTrimMark = getStreamTrimMark(Long.MAX_VALUE,
                                entry.getBackpointer(streamId), globalTrimMark);
                    }
                    Roaring64NavigableMap addressMap = new Roaring64NavigableMap();
                    addressMap.addLong(entryAddress);
                    return new StreamAddressSpace(streamTrimMark, addressMap);
                }

                if (initialize) {
                    long streamTrimMark = getStreamTrimMark(addressSpace.getTrimMark(),
                            entry.getBackpointer(streamId), globalTrimMark);
                    addressSpace.setTrimMark(streamTrimMark);
                }

                addressSpace.addAddress(entryAddress);
                return addressSpace;
            });
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

                // The trim mark is part of the address space information and is also required
                // so clients can observe updates to streams that have been completely checkpointed.
                // If we hit a checkpoint and the stream is not present in the map, the checkpointed
                // address is the last trimmed address for this stream.
                streamsAddressSpaceMap.putIfAbsent(streamId,
                        new StreamAddressSpace(streamTailAtCP, new Roaring64NavigableMap()));
            }
        }
    }

    private long getStreamTrimMark(long currentTrimMark, long backpointer, long globalTrimMark) {
        long streamTrimMark = Long.min(currentTrimMark, backpointer);
        if (streamTrimMark > globalTrimMark) {
            streamTrimMark = globalTrimMark;
        }
        return streamTrimMark;
    }

    public void updateGlobalTail(long newTail) {
        globalTail = Math.max(globalTail, newTail);
    }

    public void prefixTrim(long address) {
        log.info("prefixTrim: trim stream address maps up to address {}", address);
        for (Map.Entry<UUID, StreamAddressSpace> streamAddressMap : streamsAddressSpaceMap.entrySet()) {
            log.trace("prefixTrim: trim address space for stream {} up to trim mark {}",
                    streamAddressMap.getKey(), address);
            streamAddressMap.getValue().trim(address);
        }
    }
}
