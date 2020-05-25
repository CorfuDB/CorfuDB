package org.corfudb.logreplication.infrastructure;

import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.LockListener;

import static org.corfudb.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType.AcquireLock;
import static org.corfudb.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType.ReleaseLock;

/**
 * Lock Listener implementation for Log Replication Service.
 *
 * This represents the callback for all clients attempting to acquire the lock in the Log Replication Service.
 *
 * @author annym 05/22/2020
 */
public class LogReplicationLockListener implements LockListener {
    CorfuReplicationDiscoveryService discoveryService;

    LogReplicationLockListener(CorfuReplicationDiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Override
    public void lockAcquired(LockDataTypes.LockId lockId) {
        discoveryService.putEvent(new DiscoveryServiceEvent(AcquireLock, null));
    }

    @Override
    public void lockRevoked(LockDataTypes.LockId lockId) {
        discoveryService.putEvent(new DiscoveryServiceEvent(ReleaseLock, null));
    }

}
