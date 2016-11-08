package org.corfudb.runtime.exceptions;

import lombok.Getter;

/**
 * Created by mwei on 12/15/15.
 */
public class QuorumUnreachableException extends Exception {

    @Getter
    public int reachable;

    @Getter
    public int required;

    public QuorumUnreachableException(int reachable, int required) {
        super("Couldn't reach quorum, reachable=" + reachable + ", required=" + required);
        this.reachable = reachable;
        this.required = required;
    }
}
