package org.corfudb.runtime.exceptions;

/**
 * Created by mwei on 4/6/16.
 */
public class ObjectExistsException extends RuntimeException {
    public ObjectExistsException(long address) {
        super("Object already exists at address " + address);
    }
}
