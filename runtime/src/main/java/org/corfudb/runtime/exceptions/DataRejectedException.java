package org.corfudb.runtime.exceptions;

/**
 * Thrown when the data was rejected from the server and the stream is hinted to retry the existing value at a different position
 * Created by kspirov on 3/20/17.
 */
public class DataRejectedException extends LogUnitException {
}

