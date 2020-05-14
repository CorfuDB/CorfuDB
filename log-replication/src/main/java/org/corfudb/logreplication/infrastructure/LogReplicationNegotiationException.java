package org.corfudb.logreplication.infrastructure;

public class LogReplicationNegotiationException extends Exception {

    public LogReplicationNegotiationException(CorfuReplicationManager.LogReplicationNegotiationResult negotiationResult) {
        super(String.format("Negotiation failed with status {}", negotiationResult.name()));
    }
}
