package org.corfudb.runtime.exceptions;

import lombok.Getter;
import org.corfudb.util.NodeLocator;

/**
 * Created by mwei on 12/14/15.
 */
public class NetworkException extends RuntimeException {

    @Getter
    private final NodeLocator node;

    @Getter
    private final Type exceptionType;

    public static NetworkException buildDisconnected(NodeLocator node){
        return new NetworkException(Type.DISCONNECTED, node);
    }

    public NetworkException(Type exceptionType, NodeLocator node) {
        super(exceptionType.name() + " [endpoint=" + node.toString() + "]");
        this.exceptionType = exceptionType;
        this.node = node;
    }

    public NetworkException(Type exceptionType, String message, NodeLocator node, Throwable cause) {
        super(message + " [endpoint=" + node.toString() + "]", cause);
        this.exceptionType = exceptionType;
        this.node = node;
    }

    public enum Type {
        DISCONNECTED
    }
}
