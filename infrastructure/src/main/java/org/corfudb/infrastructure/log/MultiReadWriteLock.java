/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.corfudb.infrastructure.log;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Allows acquiring different read/write locks for different addresses.
 *
 * <p>Created by Konstantin Spirov on 1/22/2015
 */
public class MultiReadWriteLock {

    // lock references per thread
    private final ThreadLocal<LinkedList<LockMetadata>> threadLockReferences = new ThreadLocal<>();
    // all used locks
    private ConcurrentHashMap<Long, LockReference> locks = new ConcurrentHashMap<>();

    /**
     * Acquire a read lock. The recommended use of this method is in try-with-resources statement.
     *
     * @param address id of the lock to acquire.
     * @return A closable that will free the allocations for this lock if necessary
     */
    public AutoCloseableLock acquireReadLock(final Long address) {
        registerLockReference(address, false);
        ReentrantReadWriteLock lock = constructLockFor(address);
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        AtomicBoolean closed = new AtomicBoolean(false);
        return () -> {
            if (!closed.getAndSet(true)) {
                try {
                    readLock.unlock();
                    clearEventuallyLockFor(address);
                } finally {
                    deregisterLockReference(address, false);
                }
            }
        };
    }


    /**
     * Acquire a write lock. The recommended use of this method is in try-with-resources statement.
     *
     * @param address id of the lock to acquire.
     * @return A closable that will free the allocations for this lock if necessary
     */
    public AutoCloseableLock acquireWriteLock(final Long address) {
        registerLockReference(address, true);
        ReentrantReadWriteLock lock = constructLockFor(address);
        final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        AtomicBoolean closed = new AtomicBoolean(false);
        return () -> {
            if (!closed.getAndSet(true)) {
                try {
                    writeLock.unlock();
                    clearEventuallyLockFor(address);
                } finally {
                    deregisterLockReference(address, true);
                }
            }
        };
    }

    private ReentrantReadWriteLock constructLockFor(Long name) {
        return locks.compute(name, (key, ref) -> {
                    if (ref == null) {
                        ref = new LockReference(new ReentrantReadWriteLock());
                    }
                    ref.referenceCount++;
                    return ref;
                }
        ).getLock();
    }

    private void clearEventuallyLockFor(Long name) {
        locks.compute(name, (unusedLong, ref) -> {
            if (ref == null) {
                throw new IllegalStateException("Lock is wrongly used " + ref + " "
                        + System.identityHashCode(Thread.currentThread()));
            }
            ref.referenceCount--;
            if (ref.getReferenceCount() == 0) {
                return null;
            } else {
                return ref;
            }
        });
    }


    private void registerLockReference(long address, boolean writeLock) {
        LinkedList<LockMetadata> threadLocks = threadLockReferences.get();
        if (threadLocks == null) {
            threadLocks = new LinkedList<>();
            threadLockReferences.set(threadLocks);
        } else {
            LockMetadata last = threadLocks.getLast();
            if (last.getAddress() > address) {
                throw new IllegalStateException("Wrong lock acquisition order " + last.getAddress()
                        + " > " + address);
            }
            if (writeLock) {
                if (last.getAddress() == address && !last.isWriteLock()) {
                    throw new IllegalStateException("Write lock in the scope of read lock for "
                            + address);
                }
            }
        }
        threadLocks.add(new LockMetadata(address, writeLock));
    }

    private void deregisterLockReference(long address, boolean writeLock) {
        LinkedList<LockMetadata> threadLocks = threadLockReferences.get();
        LockMetadata last = threadLocks.removeLast();
        if (last.getAddress() != address || last.writeLock != writeLock) {
            throw new IllegalStateException("Wrong unlocking order");
        }
        if (threadLocks.isEmpty()) {
            threadLockReferences.set(null);
        }
    }

    public interface AutoCloseableLock extends AutoCloseable {
        @Override
        void close();

    }

    @Data
    @AllArgsConstructor
    private class LockMetadata {
        private long address;
        private boolean writeLock;
    }

    @Data
    private class LockReference {
        private ReentrantReadWriteLock lock;
        private int referenceCount;

        public LockReference(ReentrantReadWriteLock lock) {
            this.lock = lock;
        }
    }
}
