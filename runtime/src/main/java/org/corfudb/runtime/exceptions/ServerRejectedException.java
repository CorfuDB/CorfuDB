package org.corfudb.runtime.exceptions;

public class ServerRejectedException extends RuntimeException {
    public ServerRejectedException() {
        super("ServerRejectedException");
    }
}
