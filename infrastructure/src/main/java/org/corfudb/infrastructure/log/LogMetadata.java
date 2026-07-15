package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.corfudb.infrastructure.log.StreamLogFiles.RECORDS_PER_LOG_FILE;


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

    private final StreamLogDataStore dataStore;

    @Getter
    private volatile long globalTail;

    @Getter
    private final Map<UUID, StreamAddressSpace> streamsAddressSpaceMap;

    public LogMetadata(StreamLogDataStore dataStore) {
        this.globalTail = Address.NON_ADDRESS;
        this.streamsAddressSpaceMap = new HashMap<>();
        this.dataStore = dataStore;
    }

    public Map<UUID, Long> getStreamTails() {
        Map<UUID, Long> res = HashMap.newHashMap(streamsAddressSpaceMap.size());
        streamsAddressSpaceMap.forEach((uuid, space) ->
                res.put(uuid, space.getTail())
        );
        return res;
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
            updateStreamSpace(streamId, entryAddress, initialize);
        }

        // We can assume that every stream table/tag will have at least a No-op hole
        // with valid backpointers because of the following:
        // 1. The checkpointer captures a MinToken before it starts checkpointing
        // 2. The table checkpointer will emit a No-Op hope with backpointers before it writes the checkpoint
        // 3. The prefix trim will only happen after a full checkpoint cycle
        // 4. The prefix trim will always issue a trim based on the MinToken

        if (initialize && entry.isHole() && !entry.getBackpointerMap().isEmpty()) {
            for (var streamId : entry.getStreams()) {
                streamsAddressSpaceMap.compute(streamId, (id, addressSpace) -> {
                    var backPointer = entry.getBackpointer(streamId);
                    Objects.nonNull(backPointer);

                    // This metadata update path is called during node initialization (i.e., restarts) and
                    // during state transfer when redundancy is being restored on a replica. As a result, the
                    // updates for differnt parts of the log can appear out-of-order which would result in a
                    // temporarily incorrect StreamAddressSpace but not observable to external clients or
                    // sequencer bootstrap workflows. Additionally, since state transfer can interleave with prefix
                    // trim operations, the larger prefix trim address will override the min calculations below only
                    // if the bitmap is not empty.

                    if (addressSpace == null) {
                        return new StreamAddressSpace(backPointer, Collections.emptySet());
                    } else if (addressSpace.getTrimMark() < 0) {
                        addressSpace.setTrimMark(backPointer);
                    } else {
                        addressSpace.setTrimMark(Math.min(addressSpace.getTrimMark(), backPointer));
                    }
                    return addressSpace;
                });
            }
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
    private void updateStreamSpace(UUID streamId, long entryAddress, boolean initialize) {
        // Update stream address space (used for sequencer recovery), add this entry as a valid address for this stream.
        streamsAddressSpaceMap.compute(streamId, (id, addressSpace) -> {
            if (addressSpace == null) {
                // Note: stream trim mark is initialized to -6
                // its value will be computed as checkpoints for this stream are found in the log.
                // The presence of a checkpoint provides a valid trim mark for a stream.
                return new StreamAddressSpace(Address.NON_EXIST, Collections.singleton(entryAddress));
            }
            addressSpace.addAddress(entryAddress, initialize);
            return addressSpace;
        });
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

    public void syncTailSegment(long address) {
        // TODO(Maithem) since writing a record and setting the tail segment is not
        // an atomic operation, it is possible to set an incorrect tail segment. In
        // that case we will need to scan more than one segment
        updateGlobalTail(address);
        long segment = address / RECORDS_PER_LOG_FILE;

        dataStore.updateTailSegment(segment);
    }
}
