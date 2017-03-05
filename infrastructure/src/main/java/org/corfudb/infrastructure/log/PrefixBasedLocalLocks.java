/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.infrastructure.log;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Allows acquiring a separate read/write lock for the different data entries
 *
 * Created by Konstantin Spirov on 1/22/2015
 */
public class PrefixBasedLocalLocks {

    private HashMap<Long, ReentrantReadWriteLock> locks = new HashMap<>();
    public AutoCloseableLock acquireReadLock(final Long address) {
        ReentrantReadWriteLock lock = constructLockFor(address);
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        return () -> {
            readLock.unlock();
            clearEventuallyLockFor(address);
        };
    }

    public AutoCloseableLock acquireWriteLock(final Long address) {
        ReentrantReadWriteLock lock = constructLockFor(address);
        final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        return () -> {
            writeLock.unlock();
            clearEventuallyLockFor(address);
        };
    }

    private ReentrantReadWriteLock constructLockFor(Long name) {
        synchronized (locks) {
            ReentrantReadWriteLock lock = locks.get(name);
            if (lock == null) {
                lock = new ReentrantReadWriteLock();
                locks.put(name, lock);
            }
            return lock;
        }
    }

    private void clearEventuallyLockFor(Long name) {
        synchronized (locks) {
            ReentrantReadWriteLock lock = locks.get(name);
            if (lock == null)
                throw new IllegalStateException("Lock is wrongly used " + lock);
            if (!lock.isWriteLocked() && lock.getReadLockCount() == 0) {
                locks.remove(lock);
            }
        }
    }

    public interface AutoCloseableLock extends AutoCloseable {
        @Override
        void close();
    }
}
