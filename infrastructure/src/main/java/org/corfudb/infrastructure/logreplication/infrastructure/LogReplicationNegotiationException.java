package org.corfudb.infrastructure.logreplication.infrastructure;

/**
 * This class represents an exception during the stage of log replication negotiation.
 *
 * In the negotiation stage, Log Replication Servers exchange relevant information on the
 * state of replication to determine whether to continue from an old point or restart log replication.
 */
public class LogReplicationNegotiationException extends Exception {

    public LogReplicationNegotiationException(String reason) {
        super(String.format("Negotiation failed due to {}", reason));
    }
}
