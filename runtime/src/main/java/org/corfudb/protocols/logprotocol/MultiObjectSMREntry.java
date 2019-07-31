package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;



/**
 * A log entry structure which contains a collection of multiSMRentries,
 * each one contains a list of updates for one object.
 */
@SuppressWarnings("checkstyle:abbreviation")
@ToString
@Slf4j
public class MultiObjectSMREntry extends LogEntry implements ISMRConsumable {

    // map from stream-ID to a list of updates encapsulated as MultiSMREntry
    //TODO(Maithem): we should hide entryMap
    @Getter
    public Map<UUID, List<SMREntry>> entryMap = Collections.synchronizedMap(new HashMap<>());

    private Map<UUID, byte[]> streamBuffers = new HashMap<>();

    public MultiObjectSMREntry() {
        this.type = LogEntryType.MULTIOBJSMR;
    }

    /**
     * Add one SMR-update to one object's update-list.
     * @param streamID StreamID
     * @param updateEntry SMREntry to add
     */
    public void addTo(UUID streamID, SMREntry updateEntry) {
        List<SMREntry> updates = entryMap.getOrDefault(streamID, new ArrayList<>());
        updates.add(updateEntry);
        entryMap.put(streamID, updates);
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
            List<SMREntry> entries = entryMap.getOrDefault(streamID, new ArrayList<>());
            entries.addAll(multiSmrEntry);
            entryMap.put(streamID, entries);
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
        int numStreams = b.readInt();
        for (int i = 0; i < numStreams; i++) {
            UUID streamId = new UUID(b.readLong(), b.readLong());

            int updateLength = b.readInt();
            byte[] streamUpdates = new byte[updateLength];
            b.readBytes(streamUpdates);
            streamBuffers.put(streamId, streamUpdates);
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

                    int lengthIndex = b.writerIndex();
                    b.writeInt(0);
                    b.writeInt(x.getValue().size());
                    x.getValue().stream().forEach(smrEntry -> Serializers.CORFU.serialize(smrEntry, b));
                    int length = b.writerIndex() - lengthIndex - 4;
                    b.writerIndex(lengthIndex);
                    b.writeInt(length);
                    b.writerIndex(lengthIndex + length + 4);
                });
    }

    /**
     * Get the list of SMR updates for a particular object.
     * @param id StreamID
     * @return an empty list if object has no updates; a list of updates if exists
     */
    @Override
    public synchronized List<SMREntry> getSMRUpdates(UUID id) {
        if (streamBuffers.containsKey(id)) {
            byte[] streamUpdatesBuf = streamBuffers.get(id);
            ByteBuf buf =  Unpooled.wrappedBuffer(streamUpdatesBuf);
            int numUpdates = buf.readInt();
            List<SMREntry> smrEntries = new ArrayList<>(numUpdates);
            for (int update = 0; update < numUpdates; update++) {
                smrEntries.add((SMREntry) Serializers.CORFU.deserialize(buf, null));
            }
            entryMap.put(id, smrEntries);
            streamBuffers.remove(id);
        }

        return entryMap.getOrDefault(id, Collections.emptyList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEntry(ILogData entry) {
        super.setEntry(entry);
        this.getEntryMap().values().forEach(smrEntries -> {
            smrEntries.stream().forEach(smrEntry -> smrEntry.setEntry(entry));
        });
    }
}
