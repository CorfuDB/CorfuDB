package org.corfudb.runtime.smr.smrprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.smr.ISMREngine;
import org.corfudb.runtime.smr.TransactionalContext;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.JavaSerializer;
import org.corfudb.util.serializer.KryoSerializer;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

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

    /** Preferred constructor. Takes a function and converts it to a SMRCommand.
     *
     * @param function  The function to execute when the SMR engine encounters this command.
     */
    public LambdaSMRCommand(Function<T,R> function)
    {
        this();
        this.lambdaFunction = function;
    }

    /** Legacy constructor. Takes an old style function which included an options parameter
     * and converts it to an SMRCommand.
     *
     * @param function  The function to execute when the SMR engine encounters this command.
     *                  The options parameter must not be used.
     */
    @Deprecated
    public LambdaSMRCommand(BiFunction<T,?,R> function)
    {
        this();
        this.lambdaFunction = (Function<T,R> & Serializable) (obj) -> function.apply(obj, null);
    }

    @Setter
    @Getter
    Function<T,R> lambdaFunction;

    @Override
    @SuppressWarnings("unchecked")
    public R execute(T state, ISMREngine<T> engine) {
        try {
            return lambdaFunction.apply(state);
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
        lambdaFunction = (Function<T,R>) lambdaSerializer.deserialize(data);
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
