package org.corfudb.runtime.exceptions;

/**
 * An exception that is thrown when the quota of a resource has been exhausted
 *
 * Created by Maithem on 6/20/19.
 */
public class QuotaExceededException extends RuntimeException {
    public QuotaExceededException(String msg) {
        super(msg);
    }
}
