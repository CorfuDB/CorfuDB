package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

/**
 * Created by mwei on 9/17/15.
 */
@Getter
@Setter
public abstract class LogUnitMetadataMsg extends CorfuMsg implements IMetadata {


    /** A map of the metadata read from this entry */
    EnumMap<LogUnitMetadataType, Object> metadataMap;

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        bufferFromMap(buffer, metadataMap);
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        metadataMap = mapFromBuffer(buffer);
    }

    /* Utility functions */

    /** Generate a metadata map from a bytebuf
     *
     * @param buffer    The bytebuf to generate the metadata map from.
     * @return          The deserialized metadata map.
     */
    public static EnumMap<LogUnitMetadataType, Object> mapFromBuffer(ByteBuf buffer)
    {
        EnumMap<LogUnitMetadataType, Object> metadataMap =
                new EnumMap<LogUnitMetadataType, Object>(LogUnitMetadataType.class);

        byte numEntries = buffer.readByte();
        while (numEntries > 0 && buffer.isReadable())
        {
            IMetadata.LogUnitMetadataType t = IMetadata.metadataTypeMap.get(buffer.readByte());
            switch (t)
            {
                case STREAM:
                    Set<UUID> streams = new HashSet<>();
                    byte count = buffer.readByte();
                    for (int i = 0; i < count; i++)
                    {
                        streams.add(new UUID(buffer.readLong(), buffer.readLong()));
                    }
                    metadataMap.put(LogUnitMetadataType.STREAM, streams);
                    break;
                case RANK:
                    metadataMap.put(LogUnitMetadataType.RANK, buffer.readLong());
                    break;
                case BACKPOINTER_MAP: {
                    short bpEntries = buffer.readShort();
                    ImmutableMap.Builder<UUID, Long> mb = ImmutableMap.builder();
                    for (int i = 0; i < bpEntries; i++) {
                        UUID id = new UUID(buffer.readLong(), buffer.readLong());
                        Long backPointer = buffer.readLong();
                        mb.put(id, backPointer);
                    }
                    metadataMap.put(LogUnitMetadataType.BACKPOINTER_MAP, mb.build());
                }
                    break;
            }
            numEntries--;
        }

        return metadataMap;
    }

    @SuppressWarnings("unchecked")
    public static void bufferFromMap(ByteBuf buffer, EnumMap<LogUnitMetadataType, Object> metadataMap)
    {
        if (metadataMap == null) {buffer.writeByte(0);}
        else {
            buffer.writeByte(metadataMap.size());
            for (LogUnitMetadataType t : metadataMap.keySet()) {
                buffer.writeByte(t.asByte());
                switch (t) {
                    case STREAM:
                        Set<UUID> streams = (Set<UUID>) metadataMap.get(t);
                        buffer.writeByte(streams.size());
                        for (UUID id : streams) {
                            buffer.writeLong(id.getMostSignificantBits());
                            buffer.writeLong(id.getLeastSignificantBits());
                        }
                        break;
                    case RANK:
                        buffer.writeLong((Long) metadataMap.get(t));
                        break;
                    case BACKPOINTER_MAP:
                        Map<UUID, Long> backpointerMap = (Map<UUID,Long>) metadataMap.get(t);
                        buffer.writeShort(backpointerMap.size());
                        backpointerMap.entrySet().stream()
                                .forEach(e -> {
                                    buffer.writeLong(e.getKey().getMostSignificantBits());
                                    buffer.writeLong(e.getKey().getLeastSignificantBits());
                                    buffer.writeLong(e.getValue());
                                });
                        break;
                }
            }
        }
    }
}
