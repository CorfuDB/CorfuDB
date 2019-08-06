package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;


/**
 * A log entry structure which contains a collection of multiSMRentries,
 * each one contains a list of updates for one object.
 */
@SuppressWarnings("checkstyle:abbreviation")
@ToString
@Slf4j
public class SMRLogEntry extends LogEntry {

    // Map from stream-ID to a list of SMR updates to this stream.
    public Map<UUID, List<SMRRecord>> entryMap = Collections.synchronizedMap(new HashMap<>());

    public SMRLogEntry() {
        this.type = LogEntryType.SMRLOG;
    }

    public SMRLogEntry(Map<UUID, List<SMRRecord>> entryMap) {
        this.type = LogEntryType.SMRLOG;
        this.entryMap = entryMap;
    }

    /**
     * Extract a particular stream's entry from this object.
     *
     * @param streamID stream ID
     * @return the SMR Record list corresponding to streamId
     */
    private List<SMRRecord> getStreamEntry(UUID streamID) {
        return entryMap.computeIfAbsent(streamID, sid -> new ArrayList<>());
    }

    /**
     * Add one SMR-update to one object's update-list.
     *
     * @param streamID  StreamID
     * @param smrRecord SMRRecord to add
     */
    public void addTo(UUID streamID, SMRRecord smrRecord) {
        getStreamEntry(streamID).add(smrRecord);
    }

    /**
     * Merge two SMRLogEntry records.
     * Merging is done object-by-object.
     *
     * @param other object to merge
     */
    public void mergeInto(SMRLogEntry other) {
        if (other == null) {
            return;
        }

        other.getSMRUpdates().forEach((streamID, smrRecords) -> {
            getStreamEntry(streamID).addAll(smrRecords);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        int numStreams = b.readInt();
        entryMap = new HashMap<>();

        for (int i = 0; i < numStreams; i++) {
            UUID streamId = new UUID(b.readLong(), b.readLong());
            int numUpdates = b.readInt();
            List<SMRRecord> updates = new ArrayList<>(numUpdates);
            for (int j = 0; j < numUpdates; j++) {
                updates.add(SMRRecord.deserializeFromBuffer(b, rt));
            }
            entryMap.put(streamId, updates);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeInt(entryMap.size());
        entryMap.forEach((key, value) -> {
            b.writeLong(key.getMostSignificantBits());
            b.writeLong(key.getLeastSignificantBits());
            b.writeInt(value.size());
            value.forEach(record -> record.serialize(b));
        });
    }

    /**
     * Get the list of SMR updates for a particular stream.
     *
     * @param streamId stream ID of the stream that we are interested in
     * @return a list of updates, or an empty list if stream has no updates
     */
    public List<SMRRecord> getSMRUpdates(UUID streamId) {
        return entryMap.getOrDefault(streamId, Collections.emptyList());
    }

    /**
     * Get the SMR updates for all streams.
     *
     * @return a map of stream ID to the SMR updates of this stream.
     */
    public Map<UUID, List<SMRRecord>> getSMRUpdates() {
        return entryMap;
    }

    /**
     * {@inheritDoc}
     */
    public void setGlobalAddress(long address) {
        super.setGlobalAddress(address);
        entryMap.values().forEach(smrRecords -> {
            smrRecords.forEach(record -> record.setGlobalAddress(address));
        });
    }
}
