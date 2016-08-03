package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by mwei on 8/1/16.
 */
public interface CorfuPayload {

    @SuppressWarnings("unchecked")
    static <T> T fromBuffer(ByteBuf buf, Class<T> cls) {
        if (cls.isAssignableFrom(CorfuPayload.class)) {
            try {
                return (T) cls.getMethod("doFromBuffer", ByteBuf.class)
                        .invoke(null, buf);
            } catch (NoSuchMethodException | IllegalAccessException
                    | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        else if (cls.isAssignableFrom(Byte.class)) {
            return (T) Byte.valueOf(buf.readByte());
        } else if (cls.isAssignableFrom(Integer.class)) {
            return (T) Integer.valueOf(buf.readInt());
        } else if (cls.isAssignableFrom(Long.class)) {
            return (T) Long.valueOf(buf.readLong());
        } else if (cls.isAssignableFrom(Boolean.class)) {
            return (T) Boolean.valueOf(buf.readBoolean());
        } else if (cls.isAssignableFrom(Double.class)) {
            return (T) Double.valueOf(buf.readDouble());
        } else if (cls.isAssignableFrom(Float.class)) {
            return (T) Float.valueOf(buf.readFloat());
        } else if (cls.isAssignableFrom(String.class)) {
            int numBytes = buf.readInt();
            byte[] bytes = new byte[numBytes];
            buf.readBytes(bytes);
            return (T) new String(bytes);
        }
        throw new RuntimeException("Unknown class " + cls + " for deserialization");
    }

    static <T> void serialize(ByteBuf buffer, T payload) {
        // If it's a CorfuPayload, use the defined serializer.
        if (payload instanceof CorfuPayload) {
            ((CorfuPayload) payload).doSerialize(buffer);
        }
        // Otherwise serialize the primitive type.
        else if (payload instanceof Byte){
            buffer.writeByte((Byte)payload);
        }
        else if (payload instanceof Short){
            buffer.writeShort((Short)payload);
        }
        else if (payload instanceof Integer){
            buffer.writeInt((Integer)payload);
        }
        else if (payload instanceof Long){
            buffer.writeLong((Long)payload);
        }
        else if (payload instanceof Boolean){
            buffer.writeBoolean((Boolean)payload);
        }
        else if (payload instanceof Double){
            buffer.writeDouble((Double)payload);
        }
        else if (payload instanceof Float){
            buffer.writeFloat((Float)payload);
        }
        else if (payload instanceof String){
            byte[] s = ((String) payload).getBytes();
            buffer.writeInt(s.length);
            buffer.writeBytes(s);
        }
        throw new RuntimeException("Unknown class " + payload.getClass()
                + " for serialization");
    }

    void doSerialize(ByteBuf buf);
    <T extends CorfuPayload> T doFromBuffer(ByteBuf buf);
}
