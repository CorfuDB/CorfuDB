package org.corfudb.infrastructure.log.statetransfer.segment;

/**
 * A type of state transfer. PROTOCOL_READ means the state transfer will complete via replication
 * protocol. CONSISTENT_READ means the state transfer will complete via directly reading from the
 * log unit servers.
 */
public enum StateTransferType {
    PROTOCOL_READ,
    CONSISTENT_READ
}
