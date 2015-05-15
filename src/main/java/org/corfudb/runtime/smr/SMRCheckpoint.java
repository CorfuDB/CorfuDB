package org.corfudb.runtime.smr;

import org.corfudb.runtime.stream.ITimestamp;

import java.io.Serializable;

/**
 * Created by mwei on 5/6/15.
 */
public class SMRCheckpoint<T> implements Serializable {
    ITimestamp checkpoint;
    T underlyingObject;

    public SMRCheckpoint(ITimestamp ts, T object)
    {
        checkpoint = ts;
        underlyingObject = object;
    }

}
