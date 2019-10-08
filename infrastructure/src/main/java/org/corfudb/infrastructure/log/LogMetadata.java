package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    LogMetadata() {
        this.globalTail = Address.NON_ADDRESS;
        this.streamTails = new HashMap<>();
        // Compactor can prune stream address space, needs to be thread-safe here.
        this.streamsAddressSpaceMap = new ConcurrentHashMap<>();
    }

    public void update(List<LogData> entries) {
        for (LogData entry : entries) {
            long entryAddress = entry.getGlobalAddress();
            // Update log tail
            updateGlobalTail(entryAddress);
            // Update stream tail and stream address space.
            // This precludes compacted streams in this entry.
            for (UUID streamId : entry.getStreams()) {
                updateStreamSpace(streamId, entryAddress);
            }
        }
    }

    /**
     * Updates relevant info of a stream's space, concretely:
     * 1. Stream's tail, i.e., the last observed address for the stream.
     * 2. Stream's address space, i.e., space of all observed updates for the stream.
     *
     * @param streamId stream identifier
     * @param address  stream entry address
     */
    private void updateStreamSpace(UUID streamId, long address) {
        // It is safe to preclude compacted streams when updating stream tail
        // because there must be a larger address with this stream un-compacted.
        long currentStreamTail = streamTails.getOrDefault(streamId, Address.NON_ADDRESS);
        streamTails.put(streamId, Math.max(currentStreamTail, address));

        // Update stream address space (used for sequencer recovery),
        // add this entry as a valid address for this stream.
        streamsAddressSpaceMap.compute(streamId, (id, addressSpace) -> {
            if (addressSpace == null) {
                Roaring64NavigableMap addressMap = new Roaring64NavigableMap();
                addressMap.addLong(address);
                return new StreamAddressSpace(addressMap);
            }
            addressSpace.addAddress(address);
            return addressSpace;
        });
    }

    /**
     * Prune the stream address space, removing the addresses that are trimmed
     * away by compactor. This means all updates at these addresses on those
     * corresponding streams were marked garbage and compacted.
     *
     * @param addressesToPrune map of stream to a list of addresses to prune.
     */
    void pruneStreamSpace(Map<UUID, List<Long>> addressesToPrune) {
        addressesToPrune.forEach((streamId, addresses) -> {
            streamsAddressSpaceMap.compute(streamId, (id, addressSpace) -> {
                if (addressSpace == null) {
                    throw new IllegalStateException("Pruning stream space: " + id +
                            ", which does not exist in LogMetadata");
                }
                addressSpace.removeAddresses(addresses);
                return addressSpace;
            });
        });
    }

    void updateGlobalTail(long newTail) {
        globalTail = Math.max(globalTail, newTail);
    }
}
