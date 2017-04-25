package org.corfudb.runtime.exceptions;

/** This exception is thrown whenever a hole fill is required
 * by policy.
 * Created by mwei on 4/6/17.
 */
public class HoleFillRequiredException extends Exception {

    public HoleFillRequiredException(String message) {
        super(message);
    }

}
