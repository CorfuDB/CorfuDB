package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;

import java.util.*;


/**
 * A log entry sturcture which contains a collection of multiSMRentries,
 * each one contains a list of updates for one object.
 */
@ToString
@Slf4j
public class MultiObjectSMREntry extends LogEntry implements ISMRConsumable {

    // map from stream-ID to a list of updates encapsulated as MultiSMREntry
    @Getter
    public Map<UUID, MultiSMREntry> entryMap = new HashMap<>();

    public MultiObjectSMREntry() { this.type = LogEntryType.MULTIOBJSMR; }

    public MultiObjectSMREntry(Map<UUID, MultiSMREntry> entryMap) {
        this.type = LogEntryType.MULTIOBJSMR;
        this.entryMap = entryMap;
    }

    /**
     *
     * @param streamID
     * @return the MultiSMREntry corresponding to streamID
     */
    protected MultiSMREntry getStreamEntry(UUID streamID) {
        return getEntryMap().computeIfAbsent(streamID, u -> {
            return new MultiSMREntry();
        } );
    }

    /**
     * Add one SMR-update to one object's update-list
     * @param streamID
     * @param updateEntry
     */
    public void addTo(UUID streamID, SMREntry updateEntry) {
        getStreamEntry(streamID).addTo(updateEntry);
    }

    /**
     * merge two MultiObjectSMREntry records.
     * merging is done object-by-object
     * @param other
     */
    public void mergeInto(MultiObjectSMREntry other) {
        if (other == null) return;

        other.getEntryMap().forEach((streamID, MSMRentry) -> {
            getStreamEntry(streamID).mergeInto(MSMRentry);
        });
    }

    /**
     * This function provides the remaining buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);

        short numUpdates = b.readShort();
        entryMap = new HashMap<>();
        for (short i = 0; i < numUpdates; i++) {
            entryMap.put(
                    new UUID(b.readLong(), b.readLong()),
                    ((MultiSMREntry) Serializers.CORFU.deserialize(b, rt))
                    );
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeShort(entryMap.size());
        entryMap.entrySet().stream()
                .forEach(x -> {
                        b.writeLong(x.getKey().getMostSignificantBits());
                        b.writeLong(x.getKey().getLeastSignificantBits());
                        Serializers.CORFU.serialize(x.getValue(), b);});
    }

    /**
     * Get the list of SMR updates for a particular object
     * @param id
     * @return an empty list if object has no updates; a list of updates if exists
     */
    @Override
    public List<SMREntry> getSMRUpdates(UUID id) {
        MultiSMREntry entry = entryMap.get(id);
        return entryMap.get(id) == null ? Collections.emptyList() :
                entry.getUpdates();
    }

    /**
     * An underlying log entry, if present.
     *
     * @param entry
     */
    @Override
    public void setEntry(ILogData entry) {
        super.setEntry(entry);
        this.getEntryMap().values().forEach(x -> {
            x.setEntry(entry);
        });
    }
}
