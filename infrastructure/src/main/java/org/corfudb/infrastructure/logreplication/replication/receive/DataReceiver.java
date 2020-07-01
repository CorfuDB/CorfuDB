package org.corfudb.infrastructure.logreplication.replication.receive;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

/**
 * This Interface comprises Data Path receive operations for both Source and Sink.
 */
public interface DataReceiver {
    LogReplicationEntry receive(LogReplicationEntry message);
}