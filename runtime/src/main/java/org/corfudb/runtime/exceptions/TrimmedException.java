package org.corfudb.runtime.exceptions;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.view.Address;

/**
 * This exception is thrown when a client tries to read an address
 * that has been trimmed.
 */
public class TrimmedException extends LogUnitException {
    @Getter
    @Setter
    long address = Address.NON_ADDRESS;

    @Getter
    @Setter
    long snapshot = Address.NON_ADDRESS;

    public TrimmedException() {

    }

    public TrimmedException(String message) {
        super(message);
    }
}
