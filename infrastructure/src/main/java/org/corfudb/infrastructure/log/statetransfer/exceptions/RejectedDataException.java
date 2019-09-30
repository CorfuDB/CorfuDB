package org.corfudb.infrastructure.log.statetransfer.exceptions;

import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;

public class RejectedDataException extends RejectedAppendException {
    public RejectedDataException(List<LogData> dataEntries){
        super(dataEntries);
    }
}
