package org.corfudb.runtime.exceptions;

/**
 * Created by mwei on 12/11/15.
 */
public class WrongEpochException extends RuntimeException {
    final long correctEpoch;

    public WrongEpochException(long correctEpoch) {
        super("Wrong epoch. [expected=" + correctEpoch + "]");
        this.correctEpoch = correctEpoch;
    }
}
