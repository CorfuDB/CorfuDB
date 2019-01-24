package org.corfudb.runtime.exceptions.unrecoverable;

import javax.annotation.Nonnull;

/** Thrown when a Corfu Client/Server is interrupted in an unrecoverable manner.
 *  Once interrupted in this manner, Corfu can only guarantee safety if the application
 *  exits, as Corfu safety is no longer recoverable.
 *
 *  The constructors are not side-effect free, since they re-enable the interrupt flag. This is
 *  done in order to avoid situations in which a developer forgets to set interrupt flag.
 */
public class UnrecoverableCorfuInterruptedError extends UnrecoverableCorfuError {

    /** Construct a new {@link UnrecoverableCorfuInterruptedError} with a root cause and
     * re-enable the interrupt flag.
     *
     * @param cause     The root cause of the exception.
     */
    public UnrecoverableCorfuInterruptedError(@Nonnull InterruptedException cause) {
        super(cause);
        // Restore the interrupted status.
        Thread.currentThread().interrupt();
    }

    /** Construct a new {@link UnrecoverableCorfuInterruptedError} with a given message
     * and root cause and re-enable the interrupt flag.
     *
     * @param message   A message describing the exception.
     * @param cause     The root cause of the exception.
     */
    public UnrecoverableCorfuInterruptedError( @Nonnull String message,
                                               @Nonnull InterruptedException cause) {
        super(message, cause);
        // Restore the interrupted status.
        Thread.currentThread().interrupt();
    }
}
