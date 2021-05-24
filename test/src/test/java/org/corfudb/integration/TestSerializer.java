package org.corfudb.integration;

import io.netty.buffer.ByteBuf;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ISerializer;

public class TestSerializer implements ISerializer {

    private byte typeIdentifier;


    public TestSerializer(byte typeIdentifier) {
        this.typeIdentifier = typeIdentifier;
    }

    public void setType(byte type) {
        typeIdentifier = type;
    }

    @Override
    public byte getType() {
        return typeIdentifier;
    }

    @Override
    public Long deserialize(ByteBuf b, CorfuRuntime rt) {
        return b.readLong();
    }

    @Override
    public void serialize(Object object, ByteBuf buffer) {
        buffer.writeLong((Long)object);

    }
}

