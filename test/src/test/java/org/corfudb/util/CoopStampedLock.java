package org.corfudb.util;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.util.CoopScheduler;

/**
 *  Just barely enough of a StampedLock API to try to use with
 *  Corfu's VersionLockedObject.
 *
 *  Originally intended to be an almost-drop-in-replacement for
 *  StampedObject for use by VersionLockedObject.java.  In that
 *  use case, this version to allow deadlock-free use of the
 *  Coop scheduler by lambdas and other code that were locked
 *  and executed by VersionLockedObject::access.  In cases where
 *  the Coop scheduler is not configured, the regular StampedLock
 *  methods are used transparently (see NOTE below).
 *  Also, in that use case, this class, CoopScheduler, and CoopUtils
 *  were in the 'runtime' package instead of 'test'.
 *
 *  <p>This lock implementation is *not fair*.  It's worth noting
 *  that the default Java default implementation of locks and
 *  even synchronized are not fair, either.
 *
 *  <p>NOTE: This scheme can create situations were the lock is created in a
 *  non-CoopScheduler environment but then used under CoopScheduler control.
 *  As long as the two are environments are not running concurrently, we're OK.
 */

@Slf4j
public class CoopStampedLock {
    StampedLock stampedLock = new StampedLock();

    private long tstamp = 1;
    // private long numReaders = 0;
    // private List<Integer> waitingReaders = new LinkedList<>();
    private long writer = -1;
    private List<Integer> waitingWriters = new LinkedList<>();
    private boolean verbose = false;

    public CoopStampedLock() {
    }

    /** StampedLock-compatible tryOptimisticRead. */
    public long tryOptimisticRead() {
        if (CoopScheduler.getThreadMap().get() < 0) {
            return stampedLock.tryOptimisticRead();
        } else {
            if (CoopScheduler.getThreadMap().get() < 0) {
                if (verbose) {
                    System.err.printf("ERROR: tryOptimisticRead() by non-coop thread\n");
                }
                return 0;
            }
            if (writer >= 0) {
                return 0;
            } else {
                return tstamp;
            }
        }
    }

    /** StampedLock-compatible validate. */
    public boolean validate(long tstamp) {
        if (CoopScheduler.getThreadMap().get() < 0) {
            return stampedLock.validate(tstamp);
        } else {
            if (tstamp == 0) {
                return false;
            }
            return tstamp == this.tstamp;
        }
    }

    /** StampedLock-compatible tryConvertToWriteLock. */
    public long tryConvertToWriteLock(long tstamp) {
        int t = CoopScheduler.getThreadMap().get();

        if (t < 0) {
            return stampedLock.tryConvertToWriteLock(tstamp);
        } else {
            if (t < 0) {
                if (verbose) {
                    System.err.printf("ERROR: tryConvertToWriteLock() by non-coop thread\n");
                }
                return 0;
            }
            if (tstamp == 0 || tstamp != this.tstamp) {
                return 0;
            }
            return writeLock();
        }
    }

    /** StampedLock-compatible writeLock. */
    public long writeLock() {
        int t = CoopScheduler.getThreadMap().get();

        if (t < 0) {
            return stampedLock.writeLock();
        } else {
            if (t < 0) {
                if (verbose) {
                    System.err.printf("ERROR: tryConvertToWriteLock() by non-coop thread\n");
                }
                return 0;
            }
            while (writer >= 0) {
                CoopScheduler.sched();
            }
            tstamp++;
            writer = t;
            return tstamp;
        }
    }

    /** StampedLock-compatible unlock. */
    public void unlock(long tstamp) {
        int t = CoopScheduler.getThreadMap().get();

        if (t < 0) {
            stampedLock.unlock(tstamp);
        } else {
            if (t < 0) {
                if (verbose) {
                    System.err.printf("ERROR: unlock() by non-coop thread\n");
                }
                return;
            }
            if (tstamp == 0 || tstamp != this.tstamp) {
                if (verbose) {
                    System.err.printf("ERROR: unlock() with bogus tstamp: %d != correct value %d\n",
                            tstamp, this.tstamp);
                }
                return;
            }
            if (t != writer) {
                if (verbose) {
                    System.err.printf("ERROR: unlock() with correct tstamp: %d but "
                            + "caller tid %d != correct tid %d\n", this.tstamp, t, writer);
                }
                return;
            }
            tstamp++;
            writer = -1;
        }
    }
}
