package org.corfudb.infrastructure.log.statetransfer.exceptions;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;

/**
 * An exception that represents a failed write of garbage or data entries.
 */
@AllArgsConstructor
public class RejectedAppendException extends StateTransferException {
    @Getter
    private final List<LogData> dataEntries;
}
