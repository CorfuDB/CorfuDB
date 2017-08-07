package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;



/**
 * A log entry sturcture which contains a collection of multiSMRentries,
 * each one contains a list of updates for one object.
 */
@Deprecated // TODO: Add replacement method that conforms to style
@SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
@ToString
@Slf4j
public class MultiObjectSMREntry extends LogEntry implements ISMRConsumable {

    // map from stream-ID to a list of updates encapsulated as MultiSMREntry
    @Getter
    public Map<UUID, MultiSMREntry> entryMap = Collections.synchronizedMap(new HashMap<>());

    public MultiObjectSMREntry() {
        this.type = LogEntryType.MULTIOBJSMR;
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
    protected MultiSMREntry getStreamEntry(UUID streamID) {
        return getEntryMap().computeIfAbsent(streamID, u -> {
                    return new MultiSMREntry();
                }
        );
    }

    /**
     * Add one SMR-update to one object's update-list.
     * @param streamID StreamID
     * @param updateEntry SMREntry to add
     */
    public void addTo(UUID streamID, SMREntry updateEntry) {
        getStreamEntry(streamID).addTo(updateEntry);
    }

    /**
     * merge two MultiObjectSMREntry records.
     * merging is done object-by-object
     * @param other Object to merge.
     */
    public void mergeInto(MultiObjectSMREntry other) {
        if (other == null) {
            return;
        }

        other.getEntryMap().forEach((streamID, multiSmrEntry) -> {
            getStreamEntry(streamID).mergeInto(multiSmrEntry);
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

        int numUpdates = b.readInt();
        entryMap = new HashMap<>();
        for (int i = 0; i < numUpdates; i++) {
            entryMap.put(
                    new UUID(b.readLong(), b.readLong()),
                    ((MultiSMREntry) Serializers.CORFU.deserialize(b, rt)));
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeInt(entryMap.size());
        entryMap.entrySet().stream()
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
    public List<SMREntry> getSMRUpdates(UUID id) {
        MultiSMREntry entry = entryMap.get(id);
        return entryMap.get(id) == null ? Collections.emptyList() :
                entry.getUpdates();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEntry(ILogData entry) {
        super.setEntry(entry);
        this.getEntryMap().values().forEach(x -> {
            x.setEntry(entry);
        });
    }
}
