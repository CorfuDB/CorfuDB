package org.corfudb.runtime.exceptions;

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

    public StreamingException(Throwable throwable) {
        super(throwable);
    }

    public StreamingException(String message) {
        super(message);
    }
}
