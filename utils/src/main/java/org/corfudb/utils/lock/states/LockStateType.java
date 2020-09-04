package org.corfudb.utils.lock.states;

/**
 * Lock states.
 * <p>
 * Lock can be in one of the following states.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
public enum LockStateType {
    NO_LEASE,    // lock does not have the lease
    HAS_LEASE,   // lock has the lease
    STOPPED      // lock is stopped [will not try to acquire lease anymore]
}
