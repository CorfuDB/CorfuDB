package org.corfudb;

import io.netty.buffer.ByteBuf;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

/**
 * This serializer class is to be used only in tests.
 */
public class CustomSerializer implements ISerializer {
    ISerializer serializer = Serializers.getDefaultSerializer();
    private final byte type;

    public CustomSerializer(byte type) {
        this.type = type;
    }

    public byte getType() {
        return type;
    }

    public Object deserialize(ByteBuf b) {
        return serializer.deserialize(b);
    }

    public void serialize(Object o, ByteBuf b) {
        serializer.serialize(o, b);
    }
}
