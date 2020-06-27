package org.corfudb.infrastructure.logreplication.infrastructure;

/**
 * Exception thrown when there is an error in the Log Replication Discovery Service.
 *
 * @author amartinezman
 */
public class LogReplicationDiscoveryServiceException extends Throwable {

    public LogReplicationDiscoveryServiceException(String message) {
        super(message);
    }
}
