package org.corfudb.util.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

@Slf4j
public class KryoSerializer implements ISerializer {

    private static final ThreadLocal<Kryo> kryo = ThreadLocal.withInitial(Kryo::new);
    private final byte type;

    public KryoSerializer(byte type) {
        this.type = type;
    }

    @Override
    public byte getType() {
        return type;
    }

    /**
     * Deserialize an object from a given byte buffer.
     *
     * @param b The bytebuf to deserialize.
     * @return The deserialized object.
     */
    @Override
    public Object deserialize(ByteBuf b, CorfuRuntime rt) {
        try (ByteBufInputStream bbis = new ByteBufInputStream(b)) {
            try (Input input = new Input(bbis)) {
                return kryo.get().readClassAndObject(input);
            }
        } catch (IOException ie) {
            log.error("Exception during deserialization!", ie);
            throw new RuntimeException(ie);
        }
    }

    /**
     * Serialize an object into a given byte buffer.
     *
     * @param o The object to serialize.
     * @param b The bytebuf to serialize it into.
     */
    @Override
    public void serialize(Object o, ByteBuf b) {
        try (ByteBufOutputStream bbos = new ByteBufOutputStream(b)) {
            try (Output output = new Output(bbos)) {
                kryo.get().writeClassAndObject(output, o);
            }
        } catch (IOException ie) {
            log.error("Exception during serialization!", ie);
        }
    }
}
