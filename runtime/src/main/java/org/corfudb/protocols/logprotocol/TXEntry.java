package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
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
@Deprecated
public class TXEntry extends LogEntry implements ISMRConsumable {

    @Getter
    Map<UUID, TXObjectEntry> txMap;
    @Getter
    long readTimestamp;
    @Getter
    @Setter
    private transient boolean aborted;

    public TXEntry(@NonNull Map<UUID, TXObjectEntry> txMap, long readTimestamp) {
        this.type = LogEntryType.TX;
        this.txMap = txMap;
        this.readTimestamp = readTimestamp;
    }

    public boolean checkIfStreamAborts(UUID stream) {
        if (runtime.getLayoutView().getLayout().getSegments().get(
                runtime.getLayoutView().getLayout().getSegments().size() - 1)
                .getReplicationMode() == Layout.ReplicationMode.REPLEX) {
            // Starting at the stream local address of this entry, read backwards until you hit a stream entry whose
            // global address is less than snapshotTimestamp.
            if (getEntry().getLogicalAddresses().get(stream) == 0)
                return false;
            LogData curEntry = runtime.getAddressSpaceView().read(stream, getEntry().getLogicalAddresses().get(stream) - 1, 1)
                    .get(getEntry().getLogicalAddresses().get(stream) - 1);
            while (curEntry != null  && curEntry.getType() == DataType.DATA && curEntry.getGlobalAddress() > readTimestamp) {
                if (curEntry.getLogEntry(runtime).isMutation(stream)) {
                    return true;
                }

                if (curEntry.getLogicalAddresses().get(stream) == 0)
                    break;
                curEntry = runtime.getAddressSpaceView().read(stream, curEntry.getLogicalAddresses().get(stream) - 1, 1)
                        .get(curEntry.getLogicalAddresses().get(stream) - 1);
            }
            return false;
        }

        if (getEntry() != null && getEntry().hasBackpointer(stream)) {
            LogData backpointedEntry = getEntry();
            if (backpointedEntry.isFirstEntry(stream)) {
                return false;
            }
            int i = 0;

            while (
                    backpointedEntry.hasBackpointer(stream) &&
                            backpointedEntry.getGlobalAddress() > readTimestamp &&
                            !backpointedEntry.isFirstEntry(stream)) {
                i++;
                if (!backpointedEntry.getGlobalAddress().equals(getEntry().getGlobalAddress()) && //not self!
                        backpointedEntry.isLogEntry(runtime) && backpointedEntry.getLogEntry(runtime).isMutation(stream)) {
                    log.debug("TX aborted due to mutation [via backpointer]: " +
                                    "on stream {} at {}, tx is at {}, object read at {}, aborting entry was {}",
                            stream,
                            backpointedEntry.getGlobalAddress(),
                            entry.getGlobalAddress(),
                            readTimestamp,
                            backpointedEntry);
                    return true;
                }

                backpointedEntry = runtime.getAddressSpaceView()
                        .read(backpointedEntry.getBackpointer(stream));
            }
            return false;
        }

        for (long i = readTimestamp + 1; i < getEntry().getGlobalAddress(); i++) {
            // Backpointers not available, so we do a scan.
            LogData rr = runtime.getAddressSpaceView().read(i);
            if (rr.getType() ==
                    DataType.DATA &&
                    ((Set<UUID>) rr.getMetadataMap().get(IMetadata.LogUnitMetadataType.STREAM))
                            .contains(stream) && readTimestamp != i &&
                    rr.getPayload(runtime) instanceof LogEntry &&
                    ((LogEntry) rr.getPayload(runtime)).isMutation(stream)) {
                log.debug("TX aborted due to mutation on stream {} at {}, tx is at {}, object read at {}", stream,
                        i, getEntry().getGlobalAddress(), readTimestamp);
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

    public Set<UUID> getReadSet() {
        return txMap.entrySet().stream()
                .filter(e -> e.getValue().isRead())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
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

    @Override
    public List<SMREntry> getSMRUpdates(UUID id) {
        return txMap.get(id).getUpdates();
    }

    @ToString
    public static class TXObjectEntry implements ICorfuSerializable {

        @Getter
        List<SMREntry> updates;

        @Getter
        @Setter
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
                        (SMREntry) Serializers.CORFU.deserialize(b, rt));
            }
        }

        @Override
        public void serialize(ByteBuf b) {
            b.writeBoolean(read);
            b.writeShort(updates.size());
            updates.stream()
                    .forEach(x -> Serializers.CORFU.serialize(x, b));
        }
    }
}
