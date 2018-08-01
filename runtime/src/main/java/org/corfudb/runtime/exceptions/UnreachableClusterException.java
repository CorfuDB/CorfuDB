package org.corfudb.runtime.exceptions;

/**
 * Exception to be thrown by a system down handler when the runtime is stalled trying to reach an
 * unresponsive cluster.
 * Created by zlokhandwala on 7/18/18.
 */
public class UnreachableClusterException extends RuntimeException {

    public UnreachableClusterException(String message) {
        super(message);
    }
}
