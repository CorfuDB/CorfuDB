package org.corfudb.runtime.exceptions.unrecoverable;

/** {@link this} is thrown when a user attempts an operation on a runtime
 *  which has been shutdown.
 */
public class RuntimeShutdownError extends UnrecoverableCorfuError {

    /** Construct a new {@link this}.
     *
     */
    public RuntimeShutdownError() {
        super("The Corfu runtime was shutdown");
    }
}
