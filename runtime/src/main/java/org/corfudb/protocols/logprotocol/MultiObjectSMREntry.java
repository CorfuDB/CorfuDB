package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;

import java.util.*;


/**
 * Created by mwei on 9/20/16.
 */
@ToString
@NoArgsConstructor
@Slf4j
public class MultiObjectSMREntry extends LogEntry implements IDivisibleEntry, ISMRConsumable {

    @Getter
    public Map<UUID, MultiSMREntry> entryMap;

    /** Produce an entry for the given stream, or return null if there is no entry.
     *
     * @param stream    The stream to produce an entry for.
     * @return          An entry for that stream, or NULL, if there is no entry for that stream.
     */
    public LogEntry divideEntry(UUID stream) {
       return entryMap.get(stream);
    }

    public MultiObjectSMREntry(Map<UUID, MultiSMREntry> entryMap) {
        this.type = LogEntryType.MULTIOBJSMR;
        this.entryMap = entryMap;
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

    @Override
    public List<SMREntry> getSMRUpdates(UUID id) {
        return entryMap.get(id).getUpdates();
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
