package org.corfudb.runtime.exceptions;

import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;

/** A rejected LR request, never an acknowledgement of durable receipt. */
public class LogReplicationBusyException extends RuntimeException {
    private final LogReplicationBusyResponseMsg response;

    public LogReplicationBusyException(LogReplicationBusyResponseMsg response) {
        super("Log replication request rejected: " + response.getReason());
        this.response = response;
    }

    public LogReplicationBusyResponseMsg getResponse() {
        return response;
    }
}
