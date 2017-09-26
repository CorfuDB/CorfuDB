package org.corfudb.runtime.exceptions;

/** Represents an exception which the Corfu runtime cannot recover from due to
 * unexpected state.
 *
 * If this exception is thrown, the application should cleanup state and exit, since this is
 * likely due to a bug.
 */
public class UnrecoverableCorfuException extends RuntimeException {

    public UnrecoverableCorfuException(String description) {
        super(description);
    }
}
