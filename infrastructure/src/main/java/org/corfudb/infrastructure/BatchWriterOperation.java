package org.corfudb.infrastructure;

import java.util.concurrent.CompletableFuture;

import lombok.Data;

import lombok.ToString;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;

/**
 * This container is used by the logunit to add work to the batch writer. Its also used
 * to coordinate job completion.
 */
@Data
@ToString
public class BatchWriterOperation<T> {
    public enum Type {
        SHUTDOWN,
        WRITE,
        RANGE_WRITE,
        MULTI_GARBAGE_WRITE,
        RECOVERY_STATE_WRITE,
        SEAL,
        RESET,
        TAILS_QUERY,
        LOG_ADDRESS_SPACE_QUERY
    }

    private final Type type;
    private final CorfuPayloadMsg msg;
    private T resultValue;
    private final CompletableFuture<T> futureResult = new CompletableFuture<>();

    public static BatchWriterOperation<Void> SHUTDOWN = new BatchWriterOperation<>(Type.SHUTDOWN, null);
}