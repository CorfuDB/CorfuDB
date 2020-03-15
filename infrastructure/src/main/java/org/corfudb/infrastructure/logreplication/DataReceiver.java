package org.corfudb.infrastructure.logreplication;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.List;

/**
 *
 *
 */
public interface DataReceiver {

    LogReplicationEntry receive(LogReplicationEntry message);

    LogReplicationEntry receive(List<LogReplicationEntry> messages);
}