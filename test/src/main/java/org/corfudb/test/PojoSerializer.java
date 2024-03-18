package org.corfudb.test;

import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.Builder;
import lombok.Data;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ISerializer;

/**
 * Single type POJO serializer.
 */
public class PojoSerializer implements ISerializer {

    private static final int SERIALIZER_OFFSET = 29;  // Random number.
    private final Gson gson = new Gson();
    private final Class<?> clazz;

    public PojoSerializer(Class<?> clazz) {
        this.clazz = clazz;
    }

    @Override
    public byte getType() {
        return SERIALIZER_OFFSET;
    }

    @Override
    public Object deserialize(ByteBuf b, CorfuRuntime rt) {
        return gson.fromJson(new String(ByteBufUtil.getBytes(b)), clazz);
    }

    @Override
    public void serialize(Object o, ByteBuf b) {
        b.writeBytes(gson.toJson(o).getBytes());
    }

    /**
     * Sample POJO class.
     */
    @Data
    @Builder
    public static class Pojo {
        public final String payload;
    }
}
