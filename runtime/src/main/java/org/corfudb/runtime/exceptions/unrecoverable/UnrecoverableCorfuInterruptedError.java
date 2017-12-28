package org.corfudb.runtime.exceptions.unrecoverable;

import javax.annotation.Nonnull;

/** Thrown when a Corfu Client/Server is interrupted in an unrecoverable manner.
 *  Once interrupted in this manner, Corfu can only guarantee safety if the application
 *  exits, as Corfu safety is no longer recoverable.
 */
public class UnrecoverableCorfuInterruptedError extends UnrecoverableCorfuError {

    /** Construct a new {@link UnrecoverableCorfuInterruptedError} with a root cause.
     *
     * @param cause     The root cause of the exception.
     */
    public UnrecoverableCorfuInterruptedError(@Nonnull InterruptedException cause) {
        super(cause);
    }

    /** Construct a new {@link UnrecoverableCorfuInterruptedError} with a given message
     * and root cause.
     *
     * @param message   A message describing the exception.
     * @param cause     The root cause of the exception.
     */
    public UnrecoverableCorfuInterruptedError( @Nonnull String message,
                                               @Nonnull InterruptedException cause) {
        super(message, cause);
    }
}
