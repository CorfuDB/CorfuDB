package org.corfudb.infrastructure;

import lombok.Data;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.concurrent.CompletableFuture;

/**
 * Created by maithem on 11/28/16.
 */

@Data
public class BatchWriterOperation {
    public enum Type {
        SHUTDOWN,
        WRITE,
        TRIM,
        PREFIX_TRIM
    }

    private final Type type;
    private final Long address;
    private final LogData logData;
    private final CompletableFuture future;
    private Exception exception;

    public static BatchWriterOperation SHUTDOWN = new BatchWriterOperation(Type.SHUTDOWN,null, null, null);
}