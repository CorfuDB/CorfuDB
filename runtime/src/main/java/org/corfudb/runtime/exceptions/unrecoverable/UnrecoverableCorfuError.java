package org.corfudb.runtime.exceptions.unrecoverable;

import javax.annotation.Nonnull;

/** Thrown when a Corfu Client/Server encounters a condition which cannot be recovered from.
 *  Typically, the client/server should abort/quit/exit if such a condition is encountered.
 */
public class UnrecoverableCorfuError extends Error {

    /** Construct a new {@link UnrecoverableCorfuError} with a root cause.
     *
     * @param cause     The root cause of the exception.
     */
    public UnrecoverableCorfuError(@Nonnull Throwable cause) {
        super(cause);
    }

    /** Construct a new {@link UnrecoverableCorfuError} with a given message.
     *
     * @param message   A message describing the exception.
     */
    public UnrecoverableCorfuError(@Nonnull String message) {
        super(message);
    }

    /** Construct a new {@link UnrecoverableCorfuError} with a given message and root cause.
     *
     * @param message   A message describing the exception.
     * @param cause     The root cause of the exception.
     */
    public UnrecoverableCorfuError( @Nonnull String message,
        @Nonnull Throwable cause) {
        super(message, cause);
    }
}
