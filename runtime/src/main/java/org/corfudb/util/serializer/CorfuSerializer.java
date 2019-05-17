package org.corfudb.util.serializer;

import io.netty.buffer.ByteBuf;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.runtime.CorfuRuntime;

/**
 * Created by mwei on 9/29/15.
 */
public class CorfuSerializer implements ISerializer {

    private final byte type;

    /* The magic that denotes this is a corfu payload */
    final byte corfuPayloadMagic = 0x42;

    public CorfuSerializer(byte type) {
        this.type = type;
    }

    @Override
    public byte getType() {
        return type;
    }

    /**
     * Deserialize an object from a given byte buffer.
     *
     * @param b The bytebuf to deserialize.
     * @return The deserialized object.
     */
    @Override
    public Object deserialize(ByteBuf b, CorfuRuntime rt) {
        if (b.readByte() != corfuPayloadMagic) {
            byte[] bytes = new byte[b.readableBytes()];
            b.readBytes(bytes);
            return bytes;
        }
        return LogEntry.deserialize(b, rt);
    }

    /**
     * Serialize an object into a given byte buffer.
     *
     * @param o The object to serialize.
     * @param b The bytebuf to serialize it into.
     */
    @Override
    public void serialize(Object o, ByteBuf b) {
        if (o instanceof ICorfuSerializable) {
            b.writeByte(corfuPayloadMagic);
            ICorfuSerializable c = (ICorfuSerializable) o;
            c.serialize(b);
        } else if (o instanceof byte[]) {
            // Reserve byte for an empty magic byte
            b.writeByte(0);
            byte[] bytes = (byte[]) o;
            b.writeBytes(bytes);
        } else {
            throw new RuntimeException("Attempting to serialize unsupported type "
                    + o.getClass().getName() + ".");
        }
    }
}
