package org.corfudb.infrastructure;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.Data;

import org.corfudb.protocols.wireprotocol.LogData;

/**
 * Created by maithem on 11/28/16.
 */

@Data
public class BatchWriterOperation {
    public enum Type {
        SHUTDOWN,
        WRITE,
        RANGE_WRITE,
        TRIM,
        PREFIX_TRIM
    }

    private final Type type;
    private final Long address;
    private final LogData logData;
    private final List<LogData> entries;
    private final CompletableFuture future;
    private Exception exception;


    public static BatchWriterOperation SHUTDOWN = new BatchWriterOperation(Type.SHUTDOWN,
            null, null, null, null);
}