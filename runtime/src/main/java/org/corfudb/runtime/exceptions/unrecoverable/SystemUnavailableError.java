package org.corfudb.runtime.exceptions.unrecoverable;

/**
 * Created by rmichoud on 10/31/17.
 */
public class SystemUnavailableError extends UnrecoverableCorfuError {
    public SystemUnavailableError(String reason) {
        super(reason);
    }

}
