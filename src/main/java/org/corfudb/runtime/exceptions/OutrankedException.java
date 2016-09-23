package org.corfudb.runtime.exceptions;

import lombok.Getter;

/**
 * Created by mwei on 12/14/15.
 */
public class OutrankedException extends Exception {
    @Getter
    long newRank;

    public OutrankedException(long newRank) {
        super("Higher rank " + newRank + " encountered");
        this.newRank = newRank;
    }
}
