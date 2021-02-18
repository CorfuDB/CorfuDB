package org.corfudb.infrastructure;

import java.util.concurrent.CompletableFuture;
import lombok.Data;
import lombok.ToString;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;

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
        PREFIX_TRIM,
        SEAL,
        RESET,
        TAILS_QUERY,
        LOG_ADDRESS_SPACE_QUERY
    }

    private final Type type;
    private final RequestMsg request;
    private T resultValue;
    private final CompletableFuture<T> futureResult = new CompletableFuture<>();

    public static BatchWriterOperation<Void> SHUTDOWN = new BatchWriterOperation<>(Type.SHUTDOWN, null);
}
