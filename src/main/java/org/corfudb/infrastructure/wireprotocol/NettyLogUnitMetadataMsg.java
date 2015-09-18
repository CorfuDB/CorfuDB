package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.NettyLogUnitServer;

import java.util.*;

/**
 * Created by mwei on 9/17/15.
 */
@Getter
@Setter
public abstract class NettyLogUnitMetadataMsg extends NettyCorfuMsg implements IMetadata {

    /** A map of the metadata read from this entry */
    EnumMap<NettyLogUnitServer.LogUnitMetadataType, Object> metadataMap =
            new EnumMap<NettyLogUnitServer.LogUnitMetadataType, Object>(NettyLogUnitServer.LogUnitMetadataType.class);

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeByte(metadataMap.size());
        for (NettyLogUnitServer.LogUnitMetadataType t : metadataMap.keySet())
        {
            buffer.writeByte(t.asByte());
            switch (t)
            {
                case STREAM:
                    Set<UUID> streams = (Set<UUID>) metadataMap.get(t);
                    buffer.writeByte(streams.size());
                    for (UUID id : streams)
                    {
                        buffer.writeLong(id.getMostSignificantBits());
                        buffer.writeLong(id.getLeastSignificantBits());
                    }
                    break;
                case RANK:
                    buffer.writeLong((Long)metadataMap.get(t));
                    break;
            }
        }
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend NettyCorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        byte numEntries = buffer.readByte();
        while (numEntries > 0 && buffer.isReadable())
        {
            NettyLogUnitServer.LogUnitMetadataType t = NettyLogUnitServer.metadataTypeMap.get(buffer.readByte());
            switch (t)
            {
                case STREAM:
                    Set<UUID> streams = new HashSet<>();
                    byte count = buffer.readByte();
                    for (int i = 0; i < count; i++)
                    {
                        streams.add(new UUID(buffer.readLong(), buffer.readLong()));
                    }
                    metadataMap.put(NettyLogUnitServer.LogUnitMetadataType.STREAM, streams);
                    break;
                case RANK:
                    metadataMap.put(NettyLogUnitServer.LogUnitMetadataType.RANK, buffer.readLong());
                    break;
            }
            numEntries--;
        }
    }
}
