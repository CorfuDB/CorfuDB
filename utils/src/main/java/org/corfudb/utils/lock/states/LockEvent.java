package org.corfudb.utils.lock.states;

/**
 * This class represents lock events.
 * <p>
 * Lock event triggers the transition from one state to another.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
public enum LockEvent {
    LEASE_ACQUIRED,             // lease for the lock has been acquired
    LEASE_RENEWED,              // lease for the lock has been renewed
    LEASE_REVOKED,              // lease for the lock has been revoked, some other process got the lock
    LEASE_EXPIRED,              // lease for the lock has expired as it could not be renewed.
    UNRECOVERABLE_ERROR         // unrecoverable error occured
}