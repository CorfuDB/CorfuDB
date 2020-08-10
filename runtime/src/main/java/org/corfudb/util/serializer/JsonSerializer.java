package org.corfudb.util.serializer;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

/**
 * Created by mwei on 2/10/16.
 */
@Slf4j
public class JsonSerializer implements ISerializer {
    private final byte type;

    private static final Gson gson = new GsonBuilder()
            .create();

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
    public Object deserialize(ByteBuf b, CorfuRuntime rt) {
        int classNameLength = b.readShort();
        byte[] classNameBytes = new byte[classNameLength];
        b.readBytes(classNameBytes, 0, classNameLength);
        String className = new String(classNameBytes);
        if (className.equals("null")) {
            return null;
        }

        int numJsonBytes = b.readInt();
        String jsonString = b.readCharSequence(numJsonBytes, StandardCharsets.UTF_8).toString();

        try {
            return JSON.parseObject(jsonString, Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
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
        b.markWriterIndex();
        b.writeInt(0);
        String jsonOutput= JSON.toJSONString(o);
        int bytesWritten = b.writeCharSequence(jsonOutput, StandardCharsets.UTF_8);
        int endIdx = b.writerIndex();
        b.resetWriterIndex();
        b.writeInt(bytesWritten);
        b.writerIndex(endIdx);
    }
}
