package org.corfudb.infrastructure.logreplication;

/**
 * Represents the type of transport between Log Replication Servers.
 *
 * There are two types of transport:
 *      (a) Netty (default transport protocol)
 *      (b) Custom (user-defined protocol, requires adapter implementation)
 */
public enum LogReplicationTransportType {
    NETTY,
    CUSTOM
}
