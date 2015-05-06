package org.corfudb.runtime.smr;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * Created by mwei on 5/6/15.
 */
public class ReversibleSMREngineCommand<T> implements ISMREngineCommand<T>, Serializable
{

    ISMREngineCommand<T> command;
    ISMREngineCommand<T> reverseCommand;

    public ReversibleSMREngineCommand(ISMREngineCommand<T> command, ISMREngineCommand<T> reverseCommand)
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
    public void accept(T t, ISMREngine.ISMREngineOptions ismrEngineOptions)
    {
        command.accept(t, ismrEngineOptions);
    }

    public void reverse(T t, ISMREngine.ISMREngineOptions ismrEngineOptions)
    {
        reverseCommand.accept(t, ismrEngineOptions);
    }
}
