package org.corfudb.runtime.object;

import org.corfudb.runtime.object.ICorfuVersionPolicy.VersionPolicy;

import java.util.concurrent.locks.StampedLock;

public class WriteLock extends StampedLock {
    final VersionPolicy versionPolicy;
    public WriteLock(VersionPolicy versionPolicy) {
        super();
        this.versionPolicy = versionPolicy;
    }

    @Override
    public long writeLock() {
        if (versionPolicy == ICorfuVersionPolicy.BLIND) return 0;
        return super.writeLock();
    }

    @Override
    public void unlock(long ts) {
        if (versionPolicy == ICorfuVersionPolicy.BLIND) return;
        super.unlock(ts);
    }

}
