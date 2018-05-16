package org.corfudb.infrastructure;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelHandlerContext;
import lombok.Data;

import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.WriteRequest;

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
    private Long address;
    private LogData logData;
    private List<LogData> entries;
    private Exception exception;
    private CorfuPayloadMsg msg;
    private ChannelHandlerContext ctx;
    private IServerRouter router;

    public static BatchWriterOperation SHUTDOWN = new BatchWriterOperation(Type.SHUTDOWN);
}