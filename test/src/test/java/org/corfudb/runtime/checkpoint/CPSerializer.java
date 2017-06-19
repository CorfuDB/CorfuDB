package org.corfudb.runtime.checkpoint;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ISerializer;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * A special serializer for checkpoint testing
 */
public class CPSerializer implements ISerializer {
    GsonBuilder gsonBuilder = new GsonBuilder();
    Gson gson = gsonBuilder.create();

    private final byte type;

    public CPSerializer(byte type) {
        this.type = type;
    }

    public byte getType() {
        return type;
    }

    public Object deserialize(ByteBuf b, CorfuRuntime rt) {
        Type mapType = new TypeToken<Map<String, Long>>(){}.getType();

        int classNameLength = b.readShort();
        byte[] classNameBytes = new byte[classNameLength];
        b.readBytes(classNameBytes, 0, classNameLength);
        String className = new String(classNameBytes);
        boolean isMap = false;

        if (className.equals("null")) {
            return null;
        } else if (className.endsWith("HashMap")) {
            isMap = true;
        }

        try (ByteBufInputStream bbis = new ByteBufInputStream(b)) {
            try (InputStreamReader r = new InputStreamReader(bbis)) {
                if (isMap) {
                    return gson.fromJson(r, mapType);
                } else {
                    return gson.fromJson(r, Class.forName(className));
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

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
                gson.toJson(o, osw);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
