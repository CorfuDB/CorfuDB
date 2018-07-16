package org.corfudb.runtime.exceptions;


/**
 * An exception that is thrown when an rpc reaches an unavailable server that
 * needs to be retried.
 *
 * Created by Maithem on 7/17/18.
 */

public class UnavailableServerException extends RuntimeException {

    public UnavailableServerException() {
    }
}
