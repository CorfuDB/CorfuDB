package org.corfudb.runtime.exceptions;

import lombok.Getter;
import org.corfudb.util.NodeLocator;

/**
 * Created by mwei on 12/14/15.
 */
public class NetworkException extends RuntimeException {

    @Getter
    NodeLocator node;


    public NetworkException(String message, NodeLocator node) {
        super(message + " [endpoint=" + node.toString() + "]");
        this.node = node;
    }

    public NetworkException(String message, NodeLocator node, Throwable cause) {
        super(message + " [endpoint=" + node.toString() + "]", cause);
        this.node = node;
    }
}
