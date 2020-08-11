package org.corfudb.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.LockListener;

/**
 * This lock listener implementation is for testing purposes and updates
 * an observable for each acquired and revoked event.
 */
@Slf4j
public class TestLockListener implements LockListener {

    private int lockAcquiredCount = 0;
    private int lockRevokedCount = 0;

    @Getter
    private ObservableValue lockAcquired = new ObservableValue(lockAcquiredCount);

    @Getter
    private ObservableValue lockRevoked = new ObservableValue(lockRevokedCount);

    @Override
    public void lockAcquired(LockDataTypes.LockId lockId) {
        lockAcquiredCount++;
        log.debug("Lock has been acquired for {}, count={}", lockId.getLockName(), lockAcquiredCount);
        // Update observable value, indicating lock has been acquired
        lockAcquired.setValue(lockAcquiredCount);
    }

    @Override
    public void lockRevoked(LockDataTypes.LockId lockId) {
        lockRevokedCount++;
        log.debug("Lock has been revoked for {}, count={}", lockId.getLockName(), lockRevokedCount);
        // Update observable value, indicating lock has been revoked
        lockRevoked.setValue(lockRevokedCount);
    }
}
