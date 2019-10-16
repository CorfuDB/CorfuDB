package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import java.util.Set;

public class IncompleteDataReadException extends IncompleteReadException {
    public IncompleteDataReadException(Set<Long> missingAddresses) {
        super(missingAddresses);
    }
}
