package org.corfudb.runtime.smr.smrprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.smr.ISMREngine;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.JavaSerializer;
import org.corfudb.util.serializer.KryoSerializer;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Created by mwei on 9/29/15.
 */
@Slf4j
public class LambdaSMRCommand<T,R> extends SMRCommand<T,R> {

    public LambdaSMRCommand()
    {
        super();
        this.type = SMRCommandType.LAMBDA_COMMAND;
    }

    public LambdaSMRCommand(BiFunction<T,?,R> function)
    {
        this();
        this.lambdaFunction = function;
    }

    @Setter
    @Getter
    BiFunction<T,?,R> lambdaFunction;

    @Override
    @SuppressWarnings("unchecked")
    public R execute(T state, ISMREngine<T> engine) {
        try {
            return lambdaFunction.apply(state, null);
        } catch (Exception e)
        {
            log.error("Exception during execution ", e);
            throw new RuntimeException(e);
        }
    }

    //region Serializer
    static ISerializer lambdaSerializer = new JavaSerializer();

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
        int size = buffer.readInt();
        ByteBuf data = size == 0 ? null : buffer.slice(buffer.readerIndex(), size);
        lambdaFunction = (BiFunction<T,?,R>) lambdaSerializer.deserialize(data);
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        int index = buffer.writerIndex();
        buffer.writeInt(0);
        lambdaSerializer.serialize(lambdaFunction, buffer);
        buffer.setInt(index, buffer.writerIndex() - index - 4);
    }
    //endregion
}
