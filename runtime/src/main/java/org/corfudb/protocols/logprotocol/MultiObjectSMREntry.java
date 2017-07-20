package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;



/**
 * A log entry structure which contains a collection of multiSMREntries,
 * each one contains a list of updates for one object.
 */
@Deprecated // TODO: Add replacement method that conforms to style
@SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
@ToString
@Slf4j
public class MultiObjectSMREntry extends LogEntry implements ISMRConsumable {

    // map from stream-ID to a list of updates encapsulated as MultiSMREntry
    private Map<UUID, MultiSMREntry> entryMap;

    public MultiObjectSMREntry() {
        this.type = LogEntryType.MULTIOBJSMR;
        entryMap = new HashMap<>();
    }

    public MultiObjectSMREntry(Map<UUID, MultiSMREntry> entryMap) {
        this.type = LogEntryType.MULTIOBJSMR;
        this.entryMap = entryMap;
    }

    /** Extract a particular stream's entry from this object.
     *
     * @param streamID StreamID
     * @return the MultiSMREntry corresponding to streamId
     */
    synchronized private MultiSMREntry getStreamEntry(UUID streamID) {
        return entryMap.computeIfAbsent(streamID, u -> {
                    return new MultiSMREntry();
                }
        );
    }

    /**
     * Return the set of affected streams.
     * @return Set of UUIDs
     */
    synchronized public Set<UUID> getAffectedStreams() {
        return entryMap.keySet();
    }

    /**
     * Checks if the entry map is empty.
     * @return true if empty
     */
    synchronized public boolean isEmpty() {
        return entryMap.isEmpty();
    }

    /**
     * Return the set of entries that belong to this object.
     * @return set of entries
     */
    synchronized public Set<Map.Entry<UUID, MultiSMREntry>> getEntries()  {
        return entryMap.entrySet();
    }

    synchronized public MultiSMREntry getMultiSMREntry(UUID uuid) {
        return entryMap.get(uuid);
    }

    /**
     * Add one SMR-update to one object's update-list.
     * @param streamID StreamID
     * @param updateEntry SMREntry to add
     */
    synchronized public void addTo(UUID streamID, SMREntry updateEntry) {
        getStreamEntry(streamID).addTo(updateEntry);
    }

    /**
     * merge two MultiObjectSMREntry records.
     * merging is done object-by-object
     * @param other Object to merge.
     */
    synchronized public void mergeInto(MultiObjectSMREntry other) {
        if (other == null) {
            return;
        }

        other.entryMap.forEach((streamID, multiSmrEntry) -> {
            getStreamEntry(streamID).mergeInto(multiSmrEntry);
        });
    }

    /**
     * This function provides the remaining buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    synchronized void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);

        short numUpdates = b.readShort();
        entryMap = new HashMap<>();
        for (short i = 0; i < numUpdates; i++) {
            entryMap.put(
                    new UUID(b.readLong(), b.readLong()),
                    ((MultiSMREntry) Serializers.CORFU.deserialize(b, rt)));
        }
    }

    @Override
    synchronized public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeShort(entryMap.size());
        entryMap.entrySet()
                .forEach(x -> {
                    b.writeLong(x.getKey().getMostSignificantBits());
                    b.writeLong(x.getKey().getLeastSignificantBits());
                    Serializers.CORFU.serialize(x.getValue(), b);
                });
    }

    /**
     * Get the list of SMR updates for a particular object.
     * @param id StreamID
     * @return an empty list if object has no updates; a list of updates if exists
     */
    @Override
    synchronized public List<SMREntry> getSMRUpdates(UUID id) {
        MultiSMREntry entry = entryMap.get(id);
        return entryMap.get(id) == null ? Collections.emptyList() :
                entry.getUpdates();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    synchronized public void setEntry(ILogData entry) {
        super.setEntry(entry);
        this.entryMap.values().forEach(x -> {
            x.setEntry(entry);
        });
    }
}
