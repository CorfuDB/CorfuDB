package org.corfudb.runtime.smr;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * Created by mwei on 5/6/15.
 */
public class ReversibleSMREngineCommand<T ,R> implements ISMREngineCommand<T, R>, Serializable
{

    ISMREngineCommand<T,R> command;
    ISMREngineCommand<T,R> reverseCommand;

    public ReversibleSMREngineCommand(ISMREngineCommand<T,R> command, ISMREngineCommand<T,R> reverseCommand)
    {
        this.command = command;
        this.reverseCommand = reverseCommand;
    }

    /**
     * Performs this operation on the given arguments.
     *
     * @param t                 the first input argument
     * @param ismrEngineOptions the second input argument
     */
    @Override
    public R apply(T t, ISMREngine.ISMREngineOptions<T> ismrEngineOptions)
    {
        return command.apply(t, ismrEngineOptions);
    }

    public R reverse(T t, ISMREngine.ISMREngineOptions<T> ismrEngineOptions)
    {
        return reverseCommand.apply(t, ismrEngineOptions);
    }
}
