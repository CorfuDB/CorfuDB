package org.corfudb.runtime.exceptions;

import lombok.Getter;

/**
 * Created by dmalkhi on 7/21/17.
 */
public class StaleTokenException extends RuntimeException {
    @Getter
    private long correctEpoch;

    public StaleTokenException(long correctEpoch) {
        super("Stale token epoch. [expected=" + correctEpoch + "]");
        this.correctEpoch = correctEpoch;
    }
}
