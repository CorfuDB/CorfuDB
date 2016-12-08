package org.corfudb.infrastructure;

import lombok.Data;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.concurrent.CompletableFuture;

/**
 * Created by maithem on 11/28/16.
 */
@Data
public class WriteOperation {
    private final LogAddress logAddress;
    private final LogData logData;
    private final CompletableFuture future;

    public static WriteOperation SHUTDOWN = new WriteOperation(null, null, null);
}
