package org.corfudb.runtime.exceptions;

import lombok.Getter;

/**
 * The exception is thrown internally by the runtime
 * if the following race happens:
 * a token is obtained from sequencer at epoch x,
 * then reconfiguration happens and a new layout with epoch x+k is installed,
 * finally the action with epoch x resumes.
 *
 * Created by dmalkhi on 7/21/17.
 */
public class StaleTokenException extends RuntimeException {
    @Getter
    private final long correctEpoch;

    public StaleTokenException(long correctEpoch) {
        super("Stale token epoch. [expected=" + correctEpoch + "]");
        this.correctEpoch = correctEpoch;
    }
}
