package org.corfudb.infrastructure;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.Data;

import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.LogData;

/**
 * Created by maithem on 11/28/16.
 */

@Data
public class BatchWriterOperation<T> {
    public enum Type {
        SHUTDOWN,
        WRITE,
        RANGE_WRITE,
        TRIM,
        PREFIX_TRIM,
        SEAL,
        RESET,
        TAILS_QUERY
    }

    private final Type type;
    private final CorfuPayloadMsg msg;
    private Object retVal;
    private final CompletableFuture<T> futureResult = new CompletableFuture<>();

    public static BatchWriterOperation<Void> SHUTDOWN = new BatchWriterOperation<>(Type.SHUTDOWN, null);
}