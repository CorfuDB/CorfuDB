package org.corfudb.util.serializer;

import io.netty.buffer.ByteBuf;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Objects;

/**
 * Ideally, all serializers should be able to deal with any data type
 * thrown at it. This is not the case with {@link ProtobufSerializer},
 * which does not understand primitive data types.
 */
public class SafeProtobufSerializer implements ISerializer {

    final ISerializer protobufSerializer;

    public SafeProtobufSerializer(ISerializer protobufSerializer) {
        this.protobufSerializer = protobufSerializer;
    }

    @Override
    public byte getType() {
        return ProtobufSerializer.PROTOBUF_SERIALIZER_CODE;
    }

    @Override
    public Object deserialize(ByteBuf source, CorfuRuntime rt) {
        return protobufSerializer.deserialize(source, rt);
    }

    @Override
    public <T> T deserializeTyped(ByteBuf b, CorfuRuntime rt) {
        return (T) deserialize(b, rt);
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
