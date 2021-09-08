package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.logprotocol.CheckpointEntry.CheckpointEntryType;
import org.corfudb.protocols.wireprotocol.IMetadata.DataRank;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.JsonUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.List;
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

    ConcurrentHashMap<Class<?>, PayloadConstructor<?>> constructorMap = new ConcurrentHashMap<>(
            ImmutableMap.<Class<?>, PayloadConstructor<?>>builder()
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
                    .put(Layout.class, x -> {
                        int length = x.readInt();
                        byte[] byteArray = new byte[length];
                        x.readBytes(byteArray, 0, length);
                        String str = new String(byteArray, StandardCharsets.UTF_8);
                        return JsonUtils.parser.fromJson(str, Layout.class);
                    })
                    .put(DataRank.class, x -> new DataRank(x.readLong(), new UUID(x.readLong(), x.readLong())))
                    .put(CheckpointEntryType.class, x -> CheckpointEntryType.typeMap.get(x.readByte()))
                    .put(Codec.Type.class, x -> Codec.getCodecTypeById(x.readInt()))
                    .put(UUID.class, x -> new UUID(x.readLong(), x.readLong()))
                    .put(byte[].class, x -> {
                        int length = x.readInt();
                        byte[] bytes = new byte[length];
                        x.readBytes(bytes);
                        return bytes;
                    })
                    .put(ByteBuf.class, x -> {
                        int bytes = x.readInt();
                        ByteBuf b = x.retainedSlice(x.readerIndex(), bytes);
                        x.readerIndex(x.readerIndex() + bytes);
                        return b;
                    })
                    .put(StreamAddressRange.class, buffer ->
                            new StreamAddressRange(new UUID(buffer.readLong(), buffer.readLong()),
                                    buffer.readLong(), buffer.readLong()))
                    .put(StreamAddressSpace.class, buffer -> {
                        long trimMark = buffer.readLong();
                        Roaring64NavigableMap map = new Roaring64NavigableMap();
                        try (ByteBufInputStream inputStream = new ByteBufInputStream(buffer)) {
                            map.deserialize(inputStream);
                            return new StreamAddressSpace(trimMark, map);
                        } catch (IOException ioe) {
                            throw new SerializerException("Exception when attempting to " +
                                    "deserialize stream address space.", ioe);
                        }
                    })
                    .build()
    );


    /**
     * A lookup representing the context we'll use to do lookups.
     */
    Lookup lookup = MethodHandles.lookup();

    static <T> T fromBuffer(byte[] data, Class<T> cls) {
        ByteBuf buffer = Unpooled.wrappedBuffer(data);
        return fromBuffer(buffer, cls);
    }

    /**
     * Build payload from Buffer
     *
     * @param buf The buffer to deserialize.
     * @param cls The class of the payload.
     * @param <T> The type of the payload.
     * @return payload
     */
    @SuppressWarnings("unchecked")
    static <T> T fromBuffer(ByteBuf buf, Class<T> cls) {
        if (constructorMap.containsKey(cls)) {
            return (T) constructorMap.get(cls).construct(buf);
        }

        if (cls.isEnum()) {
            // we only know how to deal with enums with a typemap
            try {
                Map<Byte, T> enumMap = (Map<Byte, T>) cls.getDeclaredField("typeMap").get(null);
                constructorMap.put(cls, x -> enumMap.get(x.readByte()));
                return (T) constructorMap.get(cls).construct(buf);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException("only enums with a typeMap are supported!");
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        if (!ICorfuPayload.class.isAssignableFrom(cls)) {
            throw new RuntimeException("Unknown class " + cls + " for deserialization");
        }

        // Grab the constructor and get convert it to a lambda.
        try {
            Constructor<?> t = cls.getConstructor(ByteBuf.class);
            MethodHandle mh = lookup.unreflectConstructor(t);
            MethodType mt = MethodType.methodType(Object.class, ByteBuf.class);

            try {
                CallSite metafactory = LambdaMetafactory.metafactory(
                        lookup,
                        "construct",
                        MethodType.methodType(PayloadConstructor.class),
                        mt, mh, mh.type()
                );
                constructorMap.put(cls, (PayloadConstructor<T>) metafactory.getTarget().invokeExact());
                return (T) constructorMap.get(cls).construct(buf);
            } catch (Throwable th) {
                throw new RuntimeException(th);
            }
        } catch (NoSuchMethodException nsme) {
            throw new RuntimeException("CorfuPayloads must include a ByteBuf " +
                    "constructor! for class: " + cls.toString());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build payload from Buffer.
     */
    @SuppressWarnings("unchecked")
    static <T> T fromBuffer(ByteBuf buf, TypeToken<T> token) {
        Class<?> rawType = token.getRawType();

        if (rawType.isAssignableFrom(Map.class)) {
            return (T) mapFromBuffer(
                    buf,
                    token.resolveType(Map.class.getTypeParameters()[0]).getRawType(),
                    token.resolveType(Map.class.getTypeParameters()[1]).getRawType()
            );
        }

        if (rawType.isAssignableFrom(Set.class)) {
            return (T) setFromBuffer(
                    buf,
                    token.resolveType(Set.class.getTypeParameters()[0]).getRawType()
            );
        }

        return (T) fromBuffer(buf, rawType);
    }

    /**
     * A really simple flat map implementation. The first entry is the size of the map as an int,
     * and the next entries are each key followed by its value.
     * Maps of maps are currently not supported.
     *
     * @param buf        The buffer to deserialize.
     * @param keyClass   The class of the keys.
     * @param valueClass The class of the values.
     * @param <K>        The type of the keys.
     * @param <V>        The type of the values.
     * @return Map
     */
    static <K, V> Map<K, V> mapFromBuffer(ByteBuf buf, Class<K> keyClass, Class<V> valueClass) {
        int numEntries = buf.readInt();
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (int i = 0; i < numEntries; i++) {
            builder.put(fromBuffer(buf, keyClass), fromBuffer(buf, valueClass));
        }
        return builder.build();
    }

    /**
     * A really simple flat set implementation. The first entry is the size of the set as an int,
     * and the next entries are each value.
     *
     * @param buf        The buffer to deserialize.
     * @param valueClass The class of the values.
     * @param <V>        The type of the values.
     * @return Set of value types
     */
    static <V> Set<V> setFromBuffer(ByteBuf buf, Class<V> valueClass) {
        int numEntries = buf.readInt();
        ImmutableSet.Builder<V> builder = ImmutableSet.builder();
        for (int i = 0; i < numEntries; i++) {
            builder.add(fromBuffer(buf, valueClass));
        }
        return builder.build();
    }

    /**
     * A really simple flat list implementation. The first entry is the size of the set as an int,
     * and the next entries are each value.
     *
     * @param <V>        The type of the values.
     * @param buf        The buffer to deserialize.
     * @param valueClass The class of the values.
     * @return List of values types
     */
    static <V> List<V> listFromBuffer(ByteBuf buf, Class<V> valueClass) {
        int numEntries = buf.readInt();
        ImmutableList.Builder<V> builder = ImmutableList.builder();
        for (int i = 0; i < numEntries; i++) {
            builder.add(fromBuffer(buf, valueClass));
        }
        return builder.build();
    }

    /**
     * A really simple flat map implementation. The first entry is the size of the map as an int,
     * and the next entries are each value.
     *
     * @param buf      The buffer to deserialize.
     * @param keyClass The class of the keys.
     * @param <K>      The type of the keys
     * @param <V>      The type of the values.
     * @return Map for use with enum type keys
     */
    static <K extends Enum<K> & ITypedEnum<K>, V> EnumMap<K, V> enumMapFromBuffer(
            ByteBuf buf, Class<K> keyClass) {

        EnumMap<K, V> metadataMap = new EnumMap<>(keyClass);
        byte numEntries = buf.readByte();
        while (numEntries > 0 && buf.isReadable()) {
            K type = fromBuffer(buf, keyClass);
            V value = (V) fromBuffer(buf, type.getComponentType());
            metadataMap.put(type, value);
            numEntries--;
        }
        return metadataMap;
    }

    /**
     * Serialize a payload into a given byte buffer.
     *
     * @param payload The Payload to serialize.
     * @param buffer  The buffer to serialize it into.
     * @param <T>     The type of the payload.
     */
    @SuppressWarnings("unchecked")
    static <T> void serialize(ByteBuf buffer, T payload) {
        // If it's an ICorfuPayload, use the defined serializer.
        // Otherwise serialize the primitive type.
        if (payload instanceof ICorfuPayload) {
            ((ICorfuPayload<?>) payload).doSerialize(buffer);
        } else if (payload instanceof Byte) {
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
        } else if (payload instanceof byte[]) {
            buffer.writeInt(((byte[]) payload).length);
            buffer.writeBytes((byte[]) payload);
        } else if (payload instanceof String) {
            // and some standard non prims as well
            byte[] s = ((String) payload).getBytes();
            buffer.writeInt(s.length);
            buffer.writeBytes(s);
        } else if (payload instanceof UUID) {
            buffer.writeLong(((UUID) payload).getMostSignificantBits());
            buffer.writeLong(((UUID) payload).getLeastSignificantBits());
        } else if (payload instanceof EnumMap) {
            // and some collection types
            EnumMap<?, ?> map = (EnumMap<?, ?>) payload;
            buffer.writeByte(map.size());
            map.entrySet().forEach(x -> {
                serialize(buffer, x.getKey());
                serialize(buffer, x.getValue());
            });
        } else if (payload instanceof RangeSet) {
            Set<Range<?>> rs = (((RangeSet) payload).asRanges());
            buffer.writeInt(rs.size());
            rs.forEach(x -> {
                buffer.writeBoolean(x.upperBoundType() == BoundType.CLOSED);
                serialize(buffer, x.upperEndpoint());
                buffer.writeBoolean(x.upperBoundType() == BoundType.CLOSED);
                serialize(buffer, x.lowerEndpoint());
            });
        } else if (payload instanceof Range) {
            Range<?> r = (Range<?>) payload;
            buffer.writeBoolean(r.upperBoundType() == BoundType.CLOSED);
            serialize(buffer, r.upperEndpoint());
            buffer.writeBoolean(r.upperBoundType() == BoundType.CLOSED);
            serialize(buffer, r.lowerEndpoint());
        } else if (payload instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) payload;
            buffer.writeInt(map.size());
            map.forEach((key, value) -> {
                serialize(buffer, key);
                serialize(buffer, value);
            });
        } else if (payload instanceof Set) {
            Set<?> set = (Set<?>) payload;
            buffer.writeInt(set.size());
            set.forEach(x -> serialize(buffer, x));
        } else if (payload instanceof List) {
            List<?> list = (List<?>) payload;
            buffer.writeInt(list.size());
            list.forEach(x -> serialize(buffer, x));
        } else if (payload instanceof Layout) {
            byte[] b = JsonUtils.parser.toJson(payload).getBytes();
            buffer.writeInt(b.length);
            buffer.writeBytes(b);
        } else if (payload instanceof ByteBuf) {
            ByteBuf b = ((ByteBuf) payload).slice();
            b.resetReaderIndex();
            int bytes = b.readableBytes();
            buffer.writeInt(bytes);
            buffer.writeBytes(b, bytes);
        } else if (payload instanceof DataRank) {
            DataRank rank = (DataRank) payload;
            buffer.writeLong(rank.getRank());
            buffer.writeLong(rank.getUuid().getMostSignificantBits());
            buffer.writeLong(rank.getUuid().getLeastSignificantBits());
        } else if (payload instanceof CheckpointEntryType) {
            buffer.writeByte(((CheckpointEntryType) payload).asByte());
        } else if (payload instanceof Codec.Type) {
            buffer.writeInt(((Codec.Type) payload).getId());
        } else if (payload instanceof PriorityLevel) {
            buffer.writeByte(((PriorityLevel) payload).asByte());
        } else if (payload instanceof StreamAddressSpace) {
            StreamAddressSpace streamAddressSpace = (StreamAddressSpace) payload;
            buffer.writeLong(streamAddressSpace.getTrimMark());
            serialize(buffer, streamAddressSpace.getAddressMap());
        } else if (payload instanceof StreamAddressRange) {
            StreamAddressRange streamRange = (StreamAddressRange) payload;
            buffer.writeLong(streamRange.getStreamID().getMostSignificantBits());
            buffer.writeLong(streamRange.getStreamID().getLeastSignificantBits());
            buffer.writeLong(streamRange.getStart());
            buffer.writeLong(streamRange.getEnd());
        } else if (payload instanceof Roaring64NavigableMap) {
            Roaring64NavigableMap mrb = (Roaring64NavigableMap) payload;
            try (ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer);
                 DataOutputStream dataOutputStream =  new DataOutputStream(outputStream)){
                mrb.serialize(dataOutputStream);
            } catch (IOException ioe) {
                throw new SerializerException("Unexpected error while serializing to a byte array");
            }
        } else {
            throw new RuntimeException("Unknown class " + payload.getClass() + " for serialization");
        }
    }

    void doSerialize(ByteBuf buf);
}
