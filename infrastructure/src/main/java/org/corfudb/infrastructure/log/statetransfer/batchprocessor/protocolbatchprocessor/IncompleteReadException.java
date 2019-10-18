package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

import java.util.Set;

/**
 * An exception that represents a failed read of garbage or data entries.
 */
@AllArgsConstructor
public class IncompleteReadException extends StateTransferException {

    @Getter
    private final Set<Long> missingAddresses;
}
