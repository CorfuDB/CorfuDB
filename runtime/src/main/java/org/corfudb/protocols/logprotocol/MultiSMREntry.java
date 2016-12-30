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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * Created by amytai on 9/16/16.
 */
@ToString
@NoArgsConstructor
@Slf4j
public class MultiSMREntry extends LogEntry implements ISMRConsumable {

    @Getter
    List<SMREntry> updates;

    public MultiSMREntry(List<SMREntry> updates) {
        this.type = LogEntryType.MULTISMR;
        this.updates = updates;
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
        updates = new ArrayList<>();
        for (short i = 0; i < numUpdates; i++) {
            updates.add(
                    (SMREntry) Serializers.CORFU.deserialize(b, rt));
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeShort(updates.size());
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
        // TODO: we should check that the id matches the id of this entry,
        // but replex erases this information.
        return updates;
    }
}
