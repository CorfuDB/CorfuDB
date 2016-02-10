package org.corfudb.util.serializer;

import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.*;

/**
 * Created by mwei on 2/10/16.
 */
@Slf4j
public class JSONSerializer implements ISerializer {

    private static final Gson gson = new Gson();

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
        try (ByteBufInputStream bbis = new ByteBufInputStream(b))
        {
            try (InputStreamReader r = new InputStreamReader(bbis)) {
                return gson.fromJson(r, Class.forName(className));
            }
        }
        catch (IOException | ClassNotFoundException ie)
        {
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
        String className = o.getClass().getName();
        byte[] classNameBytes = className.getBytes();
        b.writeShort(classNameBytes.length);
        b.writeBytes(classNameBytes);
        try (ByteBufOutputStream bbos = new ByteBufOutputStream(b))
        {
            try (OutputStreamWriter osw = new OutputStreamWriter(bbos))
            {
                gson.toJson(o, osw);
            }
        }
        catch (IOException ie)
        {
            log.error("Exception during serialization!", ie);
        }
    }
}
