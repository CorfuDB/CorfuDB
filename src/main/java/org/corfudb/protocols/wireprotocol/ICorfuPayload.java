package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import org.corfudb.infrastructure.CorfuMsgHandler;

import java.lang.invoke.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mwei on 8/1/16.
 */
public interface ICorfuPayload<T> {

    @FunctionalInterface
    interface PayloadConstructor<T> {
        T construct(ByteBuf buf);
    }

    ConcurrentHashMap<Class<?>, PayloadConstructor<?>>
        constructorMap = new ConcurrentHashMap<>
            (ImmutableMap.<Class<?>, PayloadConstructor<?>>builder()
                .put(Byte.class, ByteBuf::readByte)
                .put(Integer.class, ByteBuf::readInt)
                .put(Long.class, ByteBuf::readLong)
                .put(Boolean.class, ByteBuf::readBoolean)
                .put(Double.class, ByteBuf::readDouble)
                .put(Float.class, ByteBuf::readFloat)
                .put(String.class, x -> {
                    int numBytes = x.readInt();
                    byte[] bytes = new byte[numBytes];
                    x.readBytes(bytes);
                    return new String(bytes);
                })
                .put(UUID.class, x -> new UUID(x.readLong(), x.readLong()))
                .build());



    /** A lookup representing the context we'll use to do lookups. */
    java.lang.invoke.MethodHandles.Lookup lookup = MethodHandles.lookup();

    @SuppressWarnings("unchecked")
    static <T> T fromBuffer(ByteBuf buf, Class<T> cls) {
        if (constructorMap.containsKey(cls)) {
            return (T) constructorMap.get(cls).construct(buf);
        }
        else {
            if (ICorfuPayload.class.isAssignableFrom(cls)) {
                // Grab the constructor and get convert it to a lambda.
                try {
                    Constructor t = cls.getConstructor(ByteBuf.class);
                    MethodHandle mh = lookup.unreflectConstructor(t);
                    MethodType mt = MethodType.methodType(Object.class, ByteBuf.class);
                    try {
                        constructorMap.put(cls, (PayloadConstructor<T>) LambdaMetafactory.metafactory(lookup,
                                "construct", MethodType.methodType(PayloadConstructor.class),
                                mt, mh, mh.type())
                                .getTarget().invokeExact());
                        return (T) constructorMap.get(cls).construct(buf);
                    } catch (Throwable th) {
                        throw new RuntimeException(th);
                    }
                } catch (NoSuchMethodException nsme) {
                    throw new RuntimeException("CorfuPayloads must include a ByteBuf constructor!");
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        throw new RuntimeException("Unknown class " + cls + " for deserialization");
    }

    /** A really simple flat map implementation. The first entry is the size of the map as an int,
     * and the next entries are each key followed by its value. Maps of maps are currently not supported...
     * @param buf        The buffer to deserialize.
     * @param keyClass      The class of the keys.
     * @param valueClass    The class of the values.
     * @param <K>           The type of the keys.
     * @param <V>           The type of the values.
     * @return
     */
    static <K,V> Map<K,V> mapFromBuffer(ByteBuf buf, Class<K> keyClass, Class<V> valueClass) {
        int numEntries = buf.readInt();
        ImmutableMap.Builder<K,V> builder = ImmutableMap.builder();
        for (int i = 0; i < numEntries; i++) {
            builder.put(fromBuffer(buf, keyClass), fromBuffer(buf, valueClass));
        }
        return builder.build();
    }

    /** A really simple flat set implementation. The first entry is the size of the set as an int,
     * and the next entries are each value..
     * @param buf        The buffer to deserialize.
     * @param valueClass    The class of the values.
     * @param <V>           The type of the values.
     * @return
     */
    static <V> Set<V> setFromBuffer(ByteBuf buf, Class<V> valueClass) {
        int numEntries = buf.readInt();
        ImmutableSet.Builder<V> builder = ImmutableSet.builder();
        for (int i = 0; i < numEntries; i++) {
            builder.add(fromBuffer(buf, valueClass));
        }
        return builder.build();
    }

    static <T> void serialize(ByteBuf buffer, T payload) {
        // If it's an ICorfuPayload, use the defined serializer.
        if (payload instanceof ICorfuPayload) {
            ((ICorfuPayload) payload).doSerialize(buffer);
        }
        // Otherwise serialize the primitive type.
        else if (payload instanceof Byte) {
            buffer.writeByte((Byte) payload);
        } else if (payload instanceof Short) {
            buffer.writeShort((Short) payload);
        } else if (payload instanceof Integer) {
            buffer.writeInt((Integer) payload);
        } else if (payload instanceof Long) {
            buffer.writeLong((Long) payload);
        } else if (payload instanceof Boolean) {
            buffer.writeBoolean((Boolean) payload);
        } else if (payload instanceof Double) {
            buffer.writeDouble((Double) payload);
        } else if (payload instanceof Float) {
            buffer.writeFloat((Float) payload);
        }
        // and some standard non prims as well
        else if (payload instanceof String) {
            byte[] s = ((String) payload).getBytes();
            buffer.writeInt(s.length);
            buffer.writeBytes(s);
        } else if (payload instanceof UUID) {
            buffer.writeLong(((UUID) payload).getMostSignificantBits());
            buffer.writeLong(((UUID) payload).getLeastSignificantBits());
        }
        // and some collection types
        else if (payload instanceof Map) {
            Map<?,?> map = (Map<?,?>) payload;
            buffer.writeInt(map.size());
            map.entrySet().stream().forEach(x -> {
                serialize(buffer, x.getKey());
                serialize(buffer, x.getValue());
            });
        } else if (payload instanceof Set) {
            Set<?> set = (Set<?>) payload;
            buffer.writeInt(set.size());
            set.stream().forEach(x -> {
                serialize(buffer, x);
            });
        }
        else {
            throw new RuntimeException("Unknown class " + payload.getClass()
                    + " for serialization");
        }
    }

    void doSerialize(ByteBuf buf);
}
