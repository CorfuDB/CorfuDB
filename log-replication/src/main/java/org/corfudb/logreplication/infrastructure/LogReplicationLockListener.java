package org.corfudb.logreplication.infrastructure;

import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.LockListener;

/**
 * Lock Listener implementation for Log Replication Service.
 *
 * This represents the callback for all clients attempting to acquire the lock in the Log Replication Service.
 *
 * @author annym 05/22/2020
 */
public class LogReplicationLockListener implements LockListener {

    @Override
    public void lockAcquired(LockDataTypes.LockId lockId) {

    }

    @Override
    public void lockRevoked(LockDataTypes.LockId lockId) {

    }

}
