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

    public LogMetadata() {
        this.globalTail = Address.NON_ADDRESS;
        this.streamTails = new HashMap<>();
        this.streamsAddressSpaceMap = new HashMap<>();
    }

    public void update(List<LogData> entries) {
        for (LogData entry : entries) {
            // This API is only used on range writes for state transfer
            // On state transfer we need to inspect checkpoint streams (initialize = true),
            // to avoid losing data for streams that are completely checkpointed (no actual
            // log entry for the regular stream is present in the log).
            update(entry, true);
        }
    }

    public void update(LogData entry, boolean initialize) {
        long entryAddress = entry.getGlobalAddress();
        // Update log tail
        updateGlobalTail(entryAddress);
        // For every stream present in entry update stream tail
        for (UUID streamId : entry.getStreams()) {
            updateStreamSpace(streamId, entryAddress);
        }

        // We should also consider checkpoint metadata while updating the tails and stream trim mark.
        // This is important because there could be streams which data is completely checkpointed,
        // i.e., no actual entries on the regular stream but only on the checkpoint stream.
        // If those streams are not updated with this info, then clients would observe those
        // streams as empty, which is not correct.
        if (entry.hasCheckpointMetadata()) {
            updateFromCheckpoint(entry, initialize);
        }
    }

    /**
     * Updates relevant info of a stream's space, concretely:
     * 1. Stream's tail, i.e., the last observed address for the stream.
     * 2. Stream's address space, i.e., space of all observed updates for the stream.
     *
     * @param streamId stream identifier.
     * @param entryAddress stream address.
     */
    private void updateStreamSpace(UUID streamId, long entryAddress) {
        // Update stream tails
        long currentStreamTail = streamTails.getOrDefault(streamId, Address.NON_ADDRESS);
        streamTails.put(streamId, Math.max(currentStreamTail, entryAddress));

        // Update stream address space (used for sequencer recovery), add this entry as a valid address for this stream.
        streamsAddressSpaceMap.compute(streamId, (id, addressSpace) -> {
            if (addressSpace == null) {
                Roaring64NavigableMap addressMap = new Roaring64NavigableMap();
                addressMap.addLong(entryAddress);
                // Note: stream trim mark is initialized to -6
                // its value will be computed as checkpoints for this stream are found in the log.
                // The presence of a checkpoint provides a valid trim mark for a stream.
                return new StreamAddressSpace(Address.NON_EXIST, addressMap);
            }
            addressSpace.addAddress(entryAddress);
            return addressSpace;
        });
    }

    /**
     * Update's relevant info of a stream's space from a checkpoint, concretely:
     * 1. Stream tail for those stream's that have all updates within a checkpoint.
     * 2. Stream trim mark, i.e., last observed address for a stream subsumed by a checkpoint.
     *
     * @param entry log entry
     * @param initialize true, if called on log unit initialization (full scan)
     *                   false, otherwise.
     */
    private void updateFromCheckpoint(LogData entry, boolean initialize) {
        UUID streamId = entry.getCheckpointedStreamId();
        long lastUpdateToStream = entry.getCheckpointedStreamStartLogAddress();

        if (Address.isAddress(lastUpdateToStream)) {
            // 1. Update stream tail
            long currentStreamTail = streamTails.getOrDefault(streamId, Address.NON_ADDRESS);
            streamTails.put(streamId, Math.max(currentStreamTail, lastUpdateToStream));

            // 2. Update stream trim mark
            // This is only required on initialization as on all other paths trim mark will be set by
            // explicit trimming.
            if (initialize) {
                // The trim mark is part of the address space information and is also required
                // so clients can observe updates to streams that have been completely checkpointed.
                // For instance, an empty address space with a stream trim mark != -6, requires data to be
                // loaded from a checkpoint (vs. no data ever written to this stream).

                // If we hit a checkpoint END record we can use this info to compute the stream trim mark,
                // i.e., last observed update to the stream that has already been checkpointed, hence
                // can be safely trimmed from the log.
                if (entry.getCheckpointType() == CheckpointEntry.CheckpointEntryType.END) {
                    streamsAddressSpaceMap.compute(streamId, (id, addressSpace) -> {
                        if (addressSpace == null) {
                            // If this entry still does not exist, means no updates have been observed for
                            // this stream yet. We can initialize the trim mark to the last observed update by the
                            // checkpoint. If further entries are observed they will be added to the address space.
                            return new StreamAddressSpace(lastUpdateToStream, new Roaring64NavigableMap());
                        }
                        // We will hold the maximum of these observed updates as the stream trim mark (highest
                        // checkpointed address), as this guarantees data is available in a checkpoint (safe trim mark).
                        addressSpace.setTrimMark(Long.max(addressSpace.getTrimMark(), lastUpdateToStream));
                        return addressSpace;
                    });
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
}
