package org.corfudb.util.serializer;

import com.esotericsoftware.kryo.Kryo;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import io.netty.buffer.ByteBuf;
import org.corfudb.runtime.CorfuRuntime;
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
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
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
