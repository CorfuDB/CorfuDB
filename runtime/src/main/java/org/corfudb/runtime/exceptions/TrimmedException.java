package org.corfudb.runtime.exceptions;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * This exception is thrown when a client tries to read an address
 * that has been trimmed.
 */
public class TrimmedException extends LogUnitException {
    /*
     * List of trimmed addresses.
     */
    @Getter
    @Setter
    private List<Long> trimmedAddresses;

    public TrimmedException() {
    }

    public TrimmedException(List<Long> trimmed) {
        super(String.format("Trimmed addresses {}", trimmed));
        trimmedAddresses = trimmed;
    }

    public TrimmedException(String message) {
        super(message);
    }
}
