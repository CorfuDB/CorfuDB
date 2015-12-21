package org.corfudb.runtime.exceptions;

/**
 * Created by mwei on 12/21/15.
 */
public class NoBootstrapException extends Exception {
    public NoBootstrapException() {
        super("Layout server not bootstrapped!");
    }
}
