package org.corfudb.utils.lock;

/**
 * Applications registering interest in a lock need to implement and register an instance of this interface
 * with the <class>LockClient</class>.
 * An instance of an application can assume exclusivity between a lockAcquired and lockRevoked.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
public interface LockListener {
    /**
     * Application gets this callback when a lock is acquired.
     */
    public void lockAcquired();

    /**
     * Application gets this callback when a lock is lost.
     */
    public void lockRevoked();
}
