package org.corfudb.util.serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * Created by mwei on 2/10/16.
 */
@Slf4j
public class JsonSerializer implements ISerializer {
    private final byte type;

    private static final Gson gson = new GsonBuilder().create();

    public JsonSerializer(byte type) {
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
    public Object deserialize(ByteBuf b) {
        int classNameLength = b.readShort();
        byte[] classNameBytes = new byte[classNameLength];
        b.readBytes(classNameBytes, 0, classNameLength);
        String className = new String(classNameBytes);
        if (className.equals("null")) {
            return null;
        } else {
            try (ByteBufInputStream bbis = new ByteBufInputStream(b)) {
                try (InputStreamReader r = new InputStreamReader(bbis)) {
                    return gson.fromJson(r, Class.forName(className));
                }
            } catch (IOException | ClassNotFoundException ie) {
                log.error("Exception during deserialization!", ie);
                throw new RuntimeException(ie);
            }
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
        String className = o == null ? "null" : o.getClass().getName();
        byte[] classNameBytes = className.getBytes();
        b.writeShort(classNameBytes.length);
        b.writeBytes(classNameBytes);
        if (o == null) {
            return;
        }
        try (ByteBufOutputStream bbos = new ByteBufOutputStream(b)) {
            try (OutputStreamWriter osw = new OutputStreamWriter(bbos)) {
                gson.toJson(o, o.getClass(), osw);
            }
        } catch (IOException ie) {
            log.error("Exception during serialization!", ie);
        }
    }
}
