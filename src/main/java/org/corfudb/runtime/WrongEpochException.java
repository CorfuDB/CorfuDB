package org.corfudb.runtime;

import lombok.Data;

/**
 * Created by mwei on 10/15/15.
 */
public class WrongEpochException extends Exception {
    public final long correctEpoch;

    public WrongEpochException(long correctEpoch)
    {
        super("Server indicates epoch is incorrect, correct epoch is " + correctEpoch);
        this.correctEpoch = correctEpoch;
    }
}
