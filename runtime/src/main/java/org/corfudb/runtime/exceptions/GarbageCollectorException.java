package org.corfudb.runtime.exceptions;

/**
 *
 * An exception for any exception that is thrown during
 * garbage (on the runtime) collection.
 *
 * Created by maithem on 11/16/18.
 */

public class GarbageCollectorException extends RuntimeException {
    public GarbageCollectorException(Exception e) {
        super(e);
    }
}
