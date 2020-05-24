package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.CorfuSerializer;
import org.corfudb.util.serializer.Serializers;

import static com.google.common.base.Preconditions.checkState;


/**
 * This class captures a list of updates.
 * Its primary use case is to allow a single log entry to
 * hold a sequence of updates made by a transaction, which are applied atomically.
 */
@SuppressWarnings("checkstyle:abbreviation")
@ToString
@Slf4j
@EqualsAndHashCode
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
            b.readByte(); // strip magic
            updates.add((SMREntry) SMREntry.deserialize(b, rt, isOpaque()));
        }
    }

    /**
     * Given a buffer with the logreader index pointing to a serialized MultiSMREntry, this method will
     * seek the buffer's logreader index to the end of the entry.
     */
    public static void seekToEnd(ByteBuf b) {
        // Magic
        byte magicByte = b.readByte();
        checkState(magicByte == CorfuSerializer.corfuPayloadMagic, "Not a ICorfuSerializable object");
        // container type
        byte type = b.readByte();
        checkState(type == LogEntryType.MULTISMR.asByte(), "Not a MULTISMR!");
        int numUpdates = b.readInt();
        for (int i = 0; i < numUpdates; i++) {
            SMREntry.seekToEnd(b);
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
    public void setGlobalAddress(long address) {
        super.setGlobalAddress(address);
        this.getUpdates().forEach(x -> x.setGlobalAddress(address));
    }

    @Override
    public List<SMREntry> getSMRUpdates(UUID id) {
        return updates;
    }
}
