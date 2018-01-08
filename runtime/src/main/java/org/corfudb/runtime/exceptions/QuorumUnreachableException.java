package org.corfudb.runtime.exceptions;

import lombok.Getter;

/**
 * Created by mwei on 12/15/15.
 */
public class QuorumUnreachableException extends RuntimeException {

    @Getter
    public int reachable;

    @Getter
    public int required;

    /**
     * Constuctor.
     * @param reachable number of quorum reachable
     * @param required number of required quorum
     */
    public QuorumUnreachableException(int reachable, int required) {
        super("Couldn't reach quorum, reachable=" + reachable + ", required=" + required);
        this.reachable = reachable;
        this.required = required;
    }
}
