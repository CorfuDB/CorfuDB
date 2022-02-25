package org.corfudb.compactor;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.LockListener;

/**
 * Lock Listener implementation for Distributed Compactor.
 *
 * This represents the callback for all clients attempting to acquire the lock in Distributed Compactor.
 *
 */
@Slf4j
public class DistributedCompactorLockListener implements LockListener {

    @Override
    public void lockAcquired(LockDataTypes.LockId lockId) {
        log.debug("Lock acquired id={}", lockId);
        try {
            DistributedCompactorWithLock.input(DistributedCompactorWithLock.EventStatus.LOCK_ACQUIRED);
        } catch (InterruptedException e) {
            log.warn("lockAquired got Interrupted Exception: {}", e.getCause());
        }
    }

    @Override
    public void lockRevoked(LockDataTypes.LockId lockId) {
        log.debug("Lock revoked id={}", lockId);
//        DistributedCompactorWithLock.processLockAcquire();
    }

}
