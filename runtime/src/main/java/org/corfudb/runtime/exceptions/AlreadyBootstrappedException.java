package org.corfudb.runtime.exceptions;

/**
 * Created by mwei on 6/17/16.
 */
public class AlreadyBootstrappedException extends RuntimeException {

    public AlreadyBootstrappedException() {
        super("Server is already bootstrapped");
    }

    public AlreadyBootstrappedException(String msg) {
        super(msg);
    }

}
