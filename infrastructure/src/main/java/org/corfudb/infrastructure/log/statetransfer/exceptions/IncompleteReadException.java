package org.corfudb.infrastructure.log.statetransfer.exceptions;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Set;

/**
 * An exception that represents a failed read of garbage or data entries.
 */
@AllArgsConstructor
public class IncompleteReadException extends StateTransferException {

    @Getter
    private final Set<Long> missingAddresses;
}
