package org.corfudb.runtime.exceptions;

import lombok.Getter;

/**
 * CorfuStore Streaming Exceptions are wrapped in this Exception type, so clients
 * can distinguish from streaming exceptions caused by their own processing logic
 * and those internal to Corfu, which will require a snapshot in order to recover.
 *
 * CorfuStore only allows one subscriber for a given namespace.
 * If an attempt is made to register another subscriber without terminating or unsubscribing
 * the existing subscriber on that namespace this exception will be thrown.
 *
 * @author annym
 */
public class StreamingException extends RuntimeException {

    public enum ExceptionCause {
        SUBSCRIBE_ERROR,
        TRIMMED_EXCEPTION,
        LISTENER_SUBSCRIBED
    }

    @Getter
    private final ExceptionCause exceptionCause;

    public StreamingException(Throwable throwable, ExceptionCause exceptionCause) {
        super("StreamingException: exceptionCause=" + exceptionCause, throwable);
        this.exceptionCause = exceptionCause;
    }

    public StreamingException(String message, ExceptionCause exceptionCause) {
        super("StreamingException: exceptionCause=" + exceptionCause + "\nmessage=" + message);
        this.exceptionCause = exceptionCause;
    }
}
