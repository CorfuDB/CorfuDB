package org.corfudb.runtime.exceptions;

/**
 * Can't start a transaction on a thread if there is one already in progress.
 * Hopefully in the future once we migrate away from thread-local storage
 * this exception type can disappear.
 *
 * created by hisundar 2020-09-15
 */
public class TransactionAlreadyStartedException extends RuntimeException {
    public TransactionAlreadyStartedException(String message) {
        super("An existing transaction is still in progress. " + message);
    }
}
