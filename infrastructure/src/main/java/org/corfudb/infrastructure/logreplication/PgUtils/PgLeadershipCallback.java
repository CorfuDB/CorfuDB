package org.corfudb.infrastructure.logreplication.PgUtils;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.LockListener;

import static org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType.ACQUIRE_LOCK;
import static org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType.LOCK_RENEW;
import static org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType.RELEASE_LOCK;

@Slf4j
public class PgLeadershipCallback implements LockListener {
    CorfuReplicationDiscoveryService discoveryService;

    public PgLeadershipCallback(CorfuReplicationDiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Override
    public void lockAcquired(LockDataTypes.LockId lockId) {
        log.debug("Leadership acquired!");
        discoveryService.input(new DiscoveryServiceEvent(ACQUIRE_LOCK));
    }

    @Override
    public void lockRevoked(LockDataTypes.LockId lockId) {
        log.debug("Leadership revoked!");
        discoveryService.input(new DiscoveryServiceEvent(RELEASE_LOCK));
    }

    public void lockRenewed() {
        log.debug("Leadership renewed!");
        discoveryService.input(new DiscoveryServiceEvent(LOCK_RENEW));
    }
}
