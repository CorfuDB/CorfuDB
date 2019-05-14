package org.corfudb.util.serializer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;


/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class JavaSerializer implements ISerializer {
    private final byte type;

    public JavaSerializer(byte type) {
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
            try (ObjectInputStream ois = new ObjectInputStream(bbis)) {
                return ois.readObject();
            }
        } catch (IOException | ClassNotFoundException ie) {
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
            try (ObjectOutputStream oos = new ObjectOutputStream(bbos)) {
                oos.writeObject(o);
            }
        } catch (IOException ie) {
            log.error("Exception during serialization!", ie);
        }
    }
}
