package org.corfudb.utils;

import lombok.Getter;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.LockListener;

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
        System.out.println("***** Lock has been acquired : " + lockAcquiredCount + " for: " + lockId.getLockName());
        // Update observable value, indicating lock has been acquired
        lockAcquired.setValue(lockAcquiredCount);
    }

    @Override
    public void lockRevoked(LockDataTypes.LockId lockId) {
        System.out.println("***** Lock has been revoked for: " + lockId.getLockName());
        // Update observable value, indicating lock has been revoked
        lockRevokedCount++;
        lockAcquired.setValue(lockRevokedCount);
    }
}
