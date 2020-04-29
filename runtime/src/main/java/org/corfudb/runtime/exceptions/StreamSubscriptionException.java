package org.corfudb.runtime.exceptions;

/**
 * CorfuStore only allows one subscriber for a given namespace.
 * If an attempt is made to register another subscriber without terminating or unsubscribing
 * the existing subscriber on that namespace this exception will be thrown.
 */
public class StreamSubscriptionException extends RuntimeException {
        public StreamSubscriptionException(String message) {
                super(message);
        }
}
