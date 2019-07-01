package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
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

    // Mapping between a stream and the maximum stream trim mark discovered during log scan
    @Getter
    private final Map<UUID, Long> streamTrimMarks;

    public LogMetadata() {
        this.globalTail = Address.NON_ADDRESS;
        this.streamTails = new HashMap<>();
        this.streamsAddressSpaceMap = new HashMap<>();
        this.streamTrimMarks = new HashMap<>();
    }

    public void update(List<LogData> entries) {
        for (LogData entry : entries) {
            update(entry, false);
        }
    }

    public void update(LogData entry, boolean initialize) {
        long entryAddress = entry.getGlobalAddress();
        updateGlobalTail(entryAddress);
        for (UUID streamId : entry.getStreams()) {
            // Update stream tails
            long currentStreamTail = streamTails.getOrDefault(streamId, Address.NON_ADDRESS);
            streamTails.put(streamId, Math.max(currentStreamTail, entryAddress));
            // Update stream address map (used for sequencer recovery)
            // Since entries might have been written in random order
            // We update the trim mark to be the min of all backpointer addresses.
            streamsAddressSpaceMap.compute(streamId, (id, addressSpace) -> {
                // If restarting the log unit, i.e., scanning all records in the log for initialization,
                // we build the address space for each stream. Trim mark will be set after scanning the
                // complete log, based on the observed entries in its checkpoint.
                if (addressSpace == null) {
                    Roaring64NavigableMap addressMap = new Roaring64NavigableMap();
                    addressMap.addLong(entryAddress);
                    // Note: stream trim mark is initialized to -6
                    // its actual value will be set after scanning the complete log
                    return new StreamAddressSpace(Address.NON_EXIST, addressMap);
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

                if (initialize) {
                    // The trim mark is part of the address space information and is also required
                    // so clients can observe updates to streams that have been completely checkpointed.
                    // For instance, an empty address space with a stream trim != -6, requires data to be
                    // loaded from a checkpoint (vs. no data ever written to this stream).
                    // If we hit a checkpoint END record we can use this info to compute the stream trim mark,
                    // i.e., last observed update to the stream that has already been checkpointed, hence
                    // can be safely trimmed from the log.

                    // We will hold the maximum of these observed updates as it guarantees checkpoint availability.
                    if (entry.getCheckpointType() == CheckpointEntry.CheckpointEntryType.END) {
                        streamTrimMarks.compute(streamId, (id, trimMark) -> {
                            if (trimMark == null) {
                                return streamTailAtCP;
                            }
                            // Maximum VLO version ensures highest observed checkpoint
                            return Long.max(trimMark, streamTailAtCP);
                        });
                    }
                }
            }
        }
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

    /**
     * Update stream trim marks with the values computed after the complete log has been scanned.
     */
    public void updateStreamTrimMarks() {
        log.info("updateStreamTrimMarks: set stream's trim marks on initialization.");
        streamTrimMarks.forEach((streamId, trimMark) ->
            streamsAddressSpaceMap.compute(streamId, (id, addressSpace) -> {
                if (addressSpace == null) {
                    // If this entry does not exist, it means this is a stream completely checkpointed
                    // and no updates are observed (empty address space).
                    return new StreamAddressSpace(trimMark, new Roaring64NavigableMap());
                }
                addressSpace.setTrimMark(trimMark);
                return addressSpace;
            }));
    }
}
