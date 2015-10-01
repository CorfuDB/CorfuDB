package org.corfudb.runtime.smr.smrprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.objects.CorfuObjectByteBuddyProxy;
import org.corfudb.runtime.smr.ISMREngine;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.KryoSerializer;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.function.BiFunction;

/**
 * Created by mwei on 9/30/15.
 */
@Slf4j
public class MethodTokenSMRCommand<T,R> extends SMRCommand<T,R> {

    public MethodTokenSMRCommand()
    {
        super();
        this.type = SMRCommandType.METHOD_TOKEN;
    }

    public MethodTokenSMRCommand(String functionName, Object[] arguments)
    {
        this();
        this.functionName = functionName;
        this.arguments = arguments;
    }

    @Setter
    @Getter
    String functionName;

    @Setter
    @Getter
    Object[] arguments;

    @Override
    @SuppressWarnings("unchecked")
    public R execute(T state, ISMREngine<T> engine) {
        try {
            Method m = Arrays.stream(engine.getImplementingObject().getClass().getDeclaredMethods())
                    .filter(x -> CorfuObjectByteBuddyProxy.getAccessorShortMethodNameOrEmpty(x).equals(functionName))
                    .findFirst().get();
            m.setAccessible(true);
            return (R) m.invoke(engine.getImplementingObject(), arguments);
        } catch (Exception e)
        {
            log.error("Exception during execution ", e);
            throw new RuntimeException(e);
        }
    }

    //region Serializer
    static ISerializer argumentSerializer = new KryoSerializer();

    /**
     * Parse the rest of the message from the buffer. Classes that extend SMRCommand
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    @SuppressWarnings("unchecked")
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        short stringLength = buffer.readShort();
        functionName = new String(buffer.readBytes(stringLength).array());
        byte argCount = buffer.readByte();
        arguments = new Object[argCount];
        for (byte i = 0; i < argCount; i++) {
            int size = buffer.readInt();
            ByteBuf data = size == 0 ? null : buffer.slice(buffer.readerIndex(), size);
            arguments[i] = argumentSerializer.deserialize(data);
            buffer.skipBytes(size);
        }
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeShort(functionName.getBytes().length);
        buffer.writeBytes(functionName.getBytes());
        buffer.writeByte(arguments.length);
        for (Object o : arguments)
        {
            int index = buffer.writerIndex();
            buffer.writeInt(0);
            argumentSerializer.serialize(o, buffer);
            buffer.setInt(index, buffer.writerIndex() - index - 4);
        }
    }
    //endregion
}
