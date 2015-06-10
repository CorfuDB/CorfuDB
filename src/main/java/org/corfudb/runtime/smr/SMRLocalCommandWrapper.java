package org.corfudb.runtime.smr;

import org.corfudb.runtime.stream.ITimestamp;

import java.io.Serializable;

/**
 * Created by mwei on 6/1/15.
 */
public class SMRLocalCommandWrapper<T, R> implements Serializable {
    ISMRLocalCommand<T, R> command;
    ITimestamp destination;

    public SMRLocalCommandWrapper(ISMRLocalCommand<T,R> command, ITimestamp destination)
    {
        this.command = command;
        this.destination = destination;
    }
}
