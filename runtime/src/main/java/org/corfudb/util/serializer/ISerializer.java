package org.corfudb.util.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import java.util.function.Function;

import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import net.openhft.hashing.LongHashFunction;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.Utils;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * This class represents a serializer, which takes an object and reads/writes it to a bytebuf.
 * Created by mwei on 9/17/15.
 */
public interface ISerializer {

    // Used for default cloning.
    ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            // Use an instantiator that does not require no-args
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(
                    new StdInstantiatorStrategy()));
            ImmutableListSerializer.registerSerializers(kryo);
            ImmutableSetSerializer.registerSerializers(kryo);
            ImmutableMapSerializer.registerSerializers(kryo);
            ImmutableMultimapSerializer.registerSerializers(kryo);
            // configure kryo instance, customize settings
            return kryo;
        }

        ;
    };

    byte getType();

    /**
     * Deserialize an object from a given byte buffer.
     *
     * @param b The bytebuf to deserialize.
     * @return The deserialized object.
     */
    Object deserialize(ByteBuf b, CorfuRuntime rt);

    /**
     * Serialize an object into a given byte buffer.
     *
     * @param o The object to serialize.
     * @param b The bytebuf to serialize it into.
     */
    void serialize(Object o, ByteBuf b);

    Map<Class<?>, Function<?, byte[]>> customHashingMap =
            new ConcurrentHashMap<>();

    /** Register a new custom hasher with the serializer.
     * @param cls               The class this hasher will handle.
     * @param hashFunction      The function to execute to generate the hash.
     */
    default <T> void registerCustomHasher(Class<T> cls, Function<T, byte[]> hashFunction) {
        customHashingMap.put(cls, hashFunction);
    }

    /** Get a custom hasher from the serializer.
     * @param cls       The class to obtain a hasher for.
     * @return          A function to generate a hash, or null, if no custom
     *                  hasher was registered.
     */
    default <T> Function<T, byte[]> getCustomHasher(Class<T> cls) {
        return (Function<T, byte[]>) customHashingMap.get(cls);
    }

    /** This map provides some methods for performing hashing on
     * known types.
     */
    Map<Class<?>, Function<?, byte[]>> hashConversionMap =
            ImmutableMap.<Class<?>, Function<?, byte[]>>builder()
                    .put(String.class, (String o) ->
                        Utils.longToBigEndianByteArray(LongHashFunction.xx().hashChars(o)))
                    .put(UUID.class, (UUID o) -> {
                        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
                        bb.putLong(o.getMostSignificantBits());
                        bb.putLong(o.getLeastSignificantBits());
                        return bb.array();
                    })
                    .put(Long.class, (Long o) -> Utils.longToBigEndianByteArray(o))
                    .put(Integer.class, (Integer o) -> Utils.intToBigEndianByteArray(o))
                    .put(Byte.class, (Byte o) -> new byte[]{o})
                    .put(long.class, (Long o) -> Utils.longToBigEndianByteArray(o))
                    .put(int.class, (Integer o) -> Utils.intToBigEndianByteArray(o))
                    .put(byte.class, (Byte o) -> new byte[]{o})
            .build();

    /** Given an object, generate a hash for it.
     *  This hash is used internally by Corfu for conflict resolution.
     *
     * The default implementation uses xxHash, a fast
     * non-cryptographic hash algorithm on the serialized
     * payload.
     *
     * It tries to be smart about some primitive types, not
     * serializing them if possible to generate the hashcode.
     *
     * @param o The object to hash.
     * @return  The hashed object value, as a byte array.
     */
    default byte[] hash(Object o) {
        final Class<?> cls = o.getClass();
        Function<Object, byte[]> conversionFunc =
                (Function<Object, byte[]>) hashConversionMap.get(cls);
        if (conversionFunc != null) {
            // If we know how to convert this object quickly, do that.
            return conversionFunc.apply(o);
        } else if ((conversionFunc =
                (Function<Object, byte[]>)getCustomHasher(cls)) != null) {
            // If we have a registered custom hasher, use that
            return conversionFunc.apply(o);
        } else if (o instanceof ICorfuHashable) {
            // The object implements Corfu hashable, so use that
            return ((ICorfuHashable) o).generateCorfuHash();
        } else {
            // Otherwise, revert to having xx generate a hash by using the
            // serializer then hashing.
            long hash;
            ByteBuf b = Unpooled.buffer();
            serialize(o, b);
            hash = LongHashFunction.xx().hashBytes(b.nioBuffer());
            b.release();
            return Utils.longToBigEndianByteArray(hash);
        }
    }

    /**
     * Clone an object through serialization.
     *
     * @param o The object to clone.
     * @return The cloned object.
     */
    default Object clone(Object o, CorfuRuntime rt) {
        return kryos.get().copy(o);
    }
}
