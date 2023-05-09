package org.corfudb.util.serializer;

import io.netty.buffer.ByteBuf;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Objects;

public class SafeProtobufSerializer implements ISerializer {

    final ISerializer protobufSerializer;

    public SafeProtobufSerializer(ISerializer protobufSerializer) {
        this.protobufSerializer = protobufSerializer;
    }

    @Override
    public byte getType() {
        return 0; // No-op.
    }

    @Override
    public Object deserialize(ByteBuf source, CorfuRuntime rt) {
        return protobufSerializer.deserialize(source, rt);
    }

    @Override
    public void serialize(Object object, ByteBuf target) {
        if (Objects.nonNull(PrimitiveSerializer.SerializerMap.get(object.getClass()))) {
            Serializers.PRIMITIVE.serialize(object, target);
        } else {
            protobufSerializer.serialize(object, target);
        }
    }
}
