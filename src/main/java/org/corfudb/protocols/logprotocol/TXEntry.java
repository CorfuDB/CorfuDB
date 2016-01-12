package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ICorfuSerializable;
import org.corfudb.util.serializer.Serializers;

import java.util.*;

/**
 * Created by mwei on 1/11/16.
 */
@ToString
@NoArgsConstructor
@Slf4j
public class TXEntry extends LogEntry {

    @ToString
    public static class TXObjectEntry implements ICorfuSerializable {
        @Getter
        long lastTimestamp;
        @Getter
        List<SMREntry> updates;

        public TXObjectEntry(long lastTimestamp, List<SMREntry> updates)
        {
            this.lastTimestamp = lastTimestamp;
            this.updates = updates;
        }

        public TXObjectEntry(ByteBuf b)
        {
            this.lastTimestamp = b.readLong();
            short numUpdates = b.readShort();
            updates = new ArrayList<>();
            for (short i = 0; i< numUpdates; i++)
            {
                updates.add(
                        (SMREntry) Serializers
                                .getSerializer(Serializers.SerializerType.CORFU)
                                .deserialize(b));
            }
        }

        @Override
        public void serialize(ByteBuf b) {
            b.writeLong(lastTimestamp);
            b.writeShort(updates.size());
            updates.stream()
                    .forEach(x -> Serializers
                            .getSerializer(Serializers.SerializerType.CORFU)
                            .serialize(x, b));
        }
    }

    @Getter
    Map<UUID, TXObjectEntry> txMap;

    public TXEntry(@NonNull Map<UUID,TXObjectEntry> txMap)
    {
        this.type = LogEntryType.TX;
        this.txMap = txMap;
    }


    /** Given a runtime and the timestamp of this entry, check
     * if the TX was aborted.
     * @param runtime       The runtime to use to check.
     * @param timestamp     The timestamp to use to check.
     * @return              True, if the TXEntry decision is abort.
     *                      False otherwise.
     */
    public boolean isAborted(CorfuRuntime runtime, long timestamp) {
        for (Map.Entry<UUID, TXEntry.TXObjectEntry> e : txMap.entrySet())
        {
            // If the last timestamp was MIN_VALUE, the object was never read from
            // This is the case if only pure mutators were used.
            if (e.getValue().getLastTimestamp() != Long.MIN_VALUE)
            {
                // We need to now check if this object changed since the tx proposer
                // put it in the log. This is a relatively simple check if backpointers
                // are available, but requires a scan if not.
                // Currently backpointers aren't available so we must scan.
                for (long i = e.getValue().getLastTimestamp() + 1; i < timestamp; i++)
                {
                    LogUnitReadResponseMsg.ReadResult rr = runtime.getAddressSpaceView().read(i).getResult();
                    if (rr.getResultType() ==
                            LogUnitReadResponseMsg.ReadResultType.DATA &&
                            ((Set<UUID>) rr.getMetadataMap().get(IMetadata.LogUnitMetadataType.STREAM))
                                    .contains(e.getKey()))
                    {
                        log.debug("TX aborted due to mutation on stream {} at {}, tx is at {}", e.getKey(),
                                i, timestamp);
                        return true;
                    }
                }
            }
        }
        return false;
    }
    /** Get the set of streams which will be affected by this
     * TX entry.
     * @return  The set of streams affected by this TX entry.
     */
    public Set<UUID> getAffectedStreams()
    {
        return txMap.keySet();
    }

    /**
     * This function provides the remaining buffer. Child entries
     * should initialize their contents based on the buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    void deserializeBuffer(ByteBuf b) {
        super.deserializeBuffer(b);
        short mapEntries = b.readShort();
        txMap = new HashMap<>();
        for (short i = 0; i < mapEntries; i++) {
            long uuidMSB = b.readLong();
            long uuidLSB = b.readLong();
            UUID id = new UUID(uuidMSB, uuidLSB);
            TXObjectEntry toe = new TXObjectEntry(b);
            txMap.put(id, toe);
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeShort(txMap.size());
        txMap.entrySet().stream()
                .forEach(x -> {
                    b.writeLong(x.getKey().getMostSignificantBits());
                    b.writeLong(x.getKey().getLeastSignificantBits());
                    x.getValue().serialize(b);
                });
    }
}
