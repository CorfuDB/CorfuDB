package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.LockListener;

import static org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType.ACQUIRE_LOCK;
import static org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType.RELEASE_LOCK;

/**
 * Lock Listener implementation for Log Replication Service.
 *
 * This represents the callback for all clients attempting to acquire the lock in the Log Replication Service.
 *
 * @author annym 05/22/2020
 */
@Slf4j
public class LogReplicationLockListener implements LockListener {
    CorfuReplicationDiscoveryService discoveryService;

    LogReplicationLockListener(CorfuReplicationDiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Override
    public void lockAcquired(LockDataTypes.LockId lockId) {
        log.debug("Lock acquired id={}", lockId);
        discoveryService.input(new DiscoveryServiceEvent(ACQUIRE_LOCK));
    }

    @Override
    public void lockRevoked(LockDataTypes.LockId lockId) {
        log.debug("Lock revoked id={}", lockId);
        discoveryService.input(new DiscoveryServiceEvent(RELEASE_LOCK));
    }

}
