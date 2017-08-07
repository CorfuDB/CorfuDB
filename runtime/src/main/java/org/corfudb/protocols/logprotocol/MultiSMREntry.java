package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;



/**
 * This class captures a list of updates.
 * Its primary use case is to alow a single log entry to
 * hold a sequence of updates made by a transaction, which are applied atomically.
 */
@Deprecated // TODO: Add replacement method that conforms to style
@SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
@ToString
@Slf4j
public class MultiSMREntry extends LogEntry implements ISMRConsumable {

    @Getter
    List<SMREntry> updates = Collections.synchronizedList(new ArrayList<>());

    public MultiSMREntry() {
        this.type = LogEntryType.MULTISMR;
    }

    public MultiSMREntry(List<SMREntry> updates) {
        this.type = LogEntryType.MULTISMR;
        this.updates = updates;
    }

    public void addTo(SMREntry entry) {
        getUpdates().add(entry);
    }

    public void mergeInto(MultiSMREntry other) {
        getUpdates().addAll(other.getUpdates());
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
        updates = new ArrayList<>();
        for (int i = 0; i < numUpdates; i++) {
            updates.add(
                    (SMREntry) Serializers.CORFU.deserialize(b, rt));
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeInt(updates.size());
        updates.stream()
                .forEach(x -> Serializers.CORFU.serialize(x, b));
    }

    @Override
    public void setEntry(ILogData entry) {
        super.setEntry(entry);
        this.getUpdates().forEach(x -> x.setEntry(entry));
    }

    @Override
    public List<SMREntry> getSMRUpdates(UUID id) {
        return updates;
    }
}
