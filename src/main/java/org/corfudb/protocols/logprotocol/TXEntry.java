package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ICorfuSerializable;
import org.corfudb.util.serializer.Serializers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Created by mwei on 1/11/16.
 */
@ToString(exclude = "aborted")
@NoArgsConstructor
@Slf4j
public class TXEntry extends LogEntry {

    @Getter
    Map<UUID, TXObjectEntry> txMap;
    @Getter
    long readTimestamp;
    @Getter(lazy = true)
    private final transient boolean aborted = checkAbort();

    public TXEntry(@NonNull Map<UUID, TXObjectEntry> txMap, long readTimestamp) {
        this.type = LogEntryType.TX;
        this.txMap = txMap;
        this.readTimestamp = readTimestamp;
    }

    public boolean checkIfStreamAborts(UUID stream) {
        if (getEntry() != null && getEntry().hasBackpointer(stream)) {
            ILogUnitEntry backpointedEntry = getEntry();
            if (backpointedEntry.isFirstEntry(stream)) {
                return false;
            }

            while (
                    backpointedEntry.hasBackpointer(stream) &&
                            backpointedEntry.getAddress() > readTimestamp &&
                            !backpointedEntry.isFirstEntry(stream)) {
                if (!backpointedEntry.getAddress().equals(getEntry().getAddress()) && //not self!
                        backpointedEntry.isLogEntry() && backpointedEntry.getLogEntry().isMutation(stream)) {
                    log.debug("TX aborted due to mutation [via backpointer]: " +
                                    "on stream {} at {}, tx is at {}, object read at {}, aborting entry was {}",
                            stream,
                            backpointedEntry.getAddress(),
                            entry.getAddress(),
                            readTimestamp,
                            backpointedEntry);
                    return true;
                }

                backpointedEntry = runtime.getAddressSpaceView()
                        .read(backpointedEntry.getBackpointer(stream));
            }
            return false;
        }

        for (long i = readTimestamp + 1; i < entry.getAddress(); i++) {
            // Backpointers not available, so we do a scan.
            ILogUnitEntry rr = runtime.getAddressSpaceView().read(i);
            if (rr.getResultType() ==
                    LogUnitReadResponseMsg.ReadResultType.DATA &&
                    ((Set<UUID>) rr.getMetadataMap().get(IMetadata.LogUnitMetadataType.STREAM))
                            .contains(stream) && readTimestamp != i &&
                    rr.getPayload() instanceof LogEntry &&
                    ((LogEntry) rr.getPayload()).isMutation(stream)) {
                log.debug("TX aborted due to mutation on stream {} at {}, tx is at {}, object read at {}", stream,
                        i, entry.getAddress(), readTimestamp);
                return true;
            }
        }
        return false;
    }

    public boolean checkAbort() {
        return txMap.entrySet().stream()
                .filter(e -> e.getValue().isRead())
                .anyMatch(e -> checkIfStreamAborts(e.getKey()));
    }

    /**
     * Get the set of streams which will be affected by this
     * TX entry.
     *
     * @return The set of streams affected by this TX entry.
     */
    public Set<UUID> getAffectedStreams() {
        return txMap.entrySet().stream()
                .filter(e -> e.getValue().getUpdates().size() != 0)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * This function provides the remaining buffer. Child entries
     * should initialize their contents based on the buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        readTimestamp = b.readLong();
        short mapEntries = b.readShort();
        txMap = new HashMap<>();
        for (short i = 0; i < mapEntries; i++) {
            long uuidMSB = b.readLong();
            long uuidLSB = b.readLong();
            UUID id = new UUID(uuidMSB, uuidLSB);
            TXObjectEntry toe = new TXObjectEntry(b, rt);
            txMap.put(id, toe);
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeLong(readTimestamp);
        b.writeShort(txMap.size());
        txMap.entrySet().stream()
                .forEach(x -> {
                    b.writeLong(x.getKey().getMostSignificantBits());
                    b.writeLong(x.getKey().getLeastSignificantBits());
                    x.getValue().serialize(b);
                });
    }

    /**
     * Returns whether the entry changes the contents of the stream.
     * For example, an aborted transaction does not change the content of the stream.
     *
     * @return True, if the entry changes the contents of the stream,
     * False otherwise.
     */
    @Override
    public boolean isMutation(UUID stream) {
        return !isAborted() && getAffectedStreams().contains(stream);
    }

    @ToString
    public static class TXObjectEntry implements ICorfuSerializable {

        @Getter
        List<SMREntry> updates;

        @Getter
        boolean read;

        public TXObjectEntry(List<SMREntry> updates, boolean read) {
            this.updates = updates;
            this.read = read;
        }

        public TXObjectEntry(ByteBuf b, CorfuRuntime rt) {
            read = b.readBoolean();
            short numUpdates = b.readShort();
            updates = new ArrayList<>();
            for (short i = 0; i < numUpdates; i++) {
                updates.add(
                        (SMREntry) Serializers
                                .getSerializer(Serializers.CORFU)
                                .deserialize(b, rt));
            }
        }

        @Override
        public void serialize(ByteBuf b) {
            b.writeBoolean(read);
            b.writeShort(updates.size());
            updates.stream()
                    .forEach(x -> Serializers
                            .getSerializer(Serializers.CORFU)
                            .serialize(x, b));
        }
    }
}
