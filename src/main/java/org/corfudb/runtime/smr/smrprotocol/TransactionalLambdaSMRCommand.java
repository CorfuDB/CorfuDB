package org.corfudb.runtime.smr.smrprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.smr.ISMREngine;
import org.corfudb.runtime.smr.ITransaction;
import org.corfudb.runtime.smr.TransactionalContext;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.JavaSerializer;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by mwei on 9/29/15.
 */
@Slf4j
public class TransactionalLambdaSMRCommand<T,R> extends SMRCommand<T,R> {

    public TransactionalLambdaSMRCommand()
    {
        super();
        this.type = SMRCommandType.TRANSACTIONAL_LAMBDA_COMMAND;
    }

    /** Takes a function and converts it to a SMRCommand.
     *
     * @param function  The function to execute when the SMR engine encounters this command.
     * @param transactionType The type of transaction to execute under.
     */
    public TransactionalLambdaSMRCommand(ITransaction<R> tx)
    {
        this();
        this.tx = tx;
    }

    @Setter
    @Getter
    ITransaction<R> tx;

    @Getter
    @Setter
    Class<? extends ITransaction> transactionType;

    @Override
    @SuppressWarnings("unchecked")
    public R execute(T state, ISMREngine<T> engine, ITimestamp ts) {
        try {
            tx.setInstance(instance);
            tx.setTimestamp(ts);
            return tx.executeTransaction(engine);
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
        tx = (ITransaction<R>) lambdaSerializer.deserialize(data);
        buffer.skipBytes(size);
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
        lambdaSerializer.serialize(tx, buffer);
        buffer.setInt(index, buffer.writerIndex() - index - 4);
    }
    //endregion
}
