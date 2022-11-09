package org.corfudb.runtime.exceptions;

import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.List;

/**
 * This exception is thrown when a client tries to read an address
 * that has been trimmed.
 */
public class TrimmedException extends LogUnitException {

    /*
     * This flag determines whether the operation that caused this exception
     * can retry or not.
     */
    @Getter
    @Setter
    private boolean retriable = true;

    /*
     * List of trimmed addresses.
     */
    @Getter
    private final List<Long> trimmedAddresses;

    public TrimmedException() {
        trimmedAddresses = Collections.emptyList();
    }

    public TrimmedException(long trimmed) {
        this(trimmed, String.format("Trimmed address: %s", trimmed));
    }

    public TrimmedException(long trimmed, String message) {
        super(message);
        trimmedAddresses = Collections.singletonList(trimmed);
    }

    public TrimmedException(List<Long> trimmed) {
        super(String.format("Trimmed addresses: %s", trimmed));
        trimmedAddresses = trimmed;
    }

    public TrimmedException(String message) {
        super(message);
        trimmedAddresses = Collections.emptyList();
    }
}
