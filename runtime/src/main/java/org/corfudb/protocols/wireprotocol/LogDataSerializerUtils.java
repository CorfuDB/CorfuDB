package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.logprotocol.CheckpointEntry;

import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class used by LogData to help serialize its contents.
 */
public class LogDataSerializerUtils {
    // Prevent class from being instantiated
    private LogDataSerializerUtils() {}

    @FunctionalInterface
    interface PayloadConstructor<T> {
        T construct(ByteBuf buf);
    }

    static ConcurrentHashMap<Class<?>, PayloadConstructor<?>> constructorMap = new ConcurrentHashMap<>(
            ImmutableMap.<Class<?>, PayloadConstructor<?>>builder()
                    .put(Byte.class, ByteBuf::readByte)
                    .put(Integer.class, ByteBuf::readInt)
                    .put(Long.class, ByteBuf::readLong)
                    .put(CheckpointEntry.CheckpointEntryType.class, x -> CheckpointEntry.CheckpointEntryType.typeMap.get(x.readByte()))
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
                    .build()
    );

    static ByteBuf byteBufFromBuffer(byte[] data) {
        ByteBuf buffer = Unpooled.wrappedBuffer(data);
        return fromBuffer(buffer, ByteBuf.class);
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
        } else if (cls.isEnum()) {
            // We only know how to deal with enums with a type map
            try {
                Map<Byte, T> typeMap = (Map<Byte, T>) cls.getDeclaredField("typeMap").get(null);
                constructorMap.put(cls, x -> typeMap.get(x.readByte()));
                return (T) constructorMap.get(cls).construct(buf);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException("Only enums with a typeMap are supported!");
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Unknown class " + cls + " for deserialization");
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
     * Serialize an object into a given byte buffer.
     *
     * @param buffer  The buffer to serialize it into.
     * @param payload The object to serialize.
     */
    static void serialize(ByteBuf buffer, Object payload) {
        if (payload instanceof Byte) {
            buffer.writeByte((Byte) payload);
        } else if (payload instanceof Long) {
            buffer.writeLong((Long) payload);
        } else if (payload instanceof byte[]) {
            buffer.writeInt(((byte[]) payload).length);
            buffer.writeBytes((byte[]) payload);
        } else if (payload instanceof UUID) {
            serialize(buffer, ((UUID) payload).getMostSignificantBits());
            serialize(buffer, ((UUID) payload).getLeastSignificantBits());
        } else if (payload instanceof EnumMap) {
            EnumMap<?, ?> map = (EnumMap<?, ?>) payload;
            buffer.writeByte(map.size());
            map.forEach((key, value) -> {
                serialize(buffer, key);
                serialize(buffer, value);
            });
        } else if (payload instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) payload;
            buffer.writeInt(map.size());
            map.forEach((key, value) -> {
                serialize(buffer, key);
                serialize(buffer, value);
            });
        } else if (payload instanceof ByteBuf) {
            ByteBuf b = ((ByteBuf) payload).slice();
            b.resetReaderIndex();
            int bytes = b.readableBytes();
            buffer.writeInt(bytes);
            buffer.writeBytes(b, bytes);
        } else if (payload instanceof  IMetadata.LogUnitMetadataType) {
            serialize(buffer, ((IMetadata.LogUnitMetadataType) payload).asByte());
        } else if (payload instanceof  CheckpointEntry.CheckpointEntryType) {
            serialize(buffer, ((CheckpointEntry.CheckpointEntryType) payload).asByte());
        } else if (payload instanceof Codec.Type) {
            buffer.writeInt(((Codec.Type) payload).getId());
        } else {
            throw new IllegalArgumentException("Unknown class " + payload.getClass() + " for serialization");
        }
    }
}
