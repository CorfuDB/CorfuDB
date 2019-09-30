package org.corfudb.infrastructure.log.statetransfer.exceptions;

import java.util.Set;

public class IncompleteDataReadException extends IncompleteReadException {
    public IncompleteDataReadException(Set<Long> missingAddresses) {
        super(missingAddresses);
    }
}
