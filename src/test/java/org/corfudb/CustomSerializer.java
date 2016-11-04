package org.corfudb;

import io.netty.buffer.ByteBuf;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

/**
 * This serializer class is to be used only in tests.
 */
public class CustomSerializer implements ISerializer {
    ISerializer serializer = Serializers.JSON;
    private final byte type;

    public CustomSerializer(byte type) {
        this.type = type;
    }

    public byte getType() {
        return type;
    }

    public Object deserialize(ByteBuf b, CorfuRuntime rt) {
        return serializer.deserialize(b, rt);
    }

    public void serialize(Object o, ByteBuf b) {
        serializer.serialize(o, b);
    }
}
