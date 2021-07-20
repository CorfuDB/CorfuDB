package org.corfudb.utils.lock.persistence;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.*;
import org.corfudb.utils.CommonTypes.Uuid;
import org.corfudb.utils.lock.LockDataTypes.LockData;
import org.corfudb.utils.lock.LockDataTypes.LockId;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.utils.lock.Lock.leaseDuration;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Enables instances of <class>Lock</class> to acquire locks and renew leases.
 * It is backed by a <class>CorfuStore</class>. The store is used for persisting the leases and also works as a
 * coordination mechanism between the distributed lock instances.
 * <p>
 * If a lock is acquired by a <class>Lock</class> instance, all the other <class>Lock</class> instances contending
 * for the lock will not acquire the same lock until the lease expires. The lessee is expected to renew/update
 * the lease in order to keep holding the lock.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
//TODO Add logging everywhere
@Slf4j
public class LockStore {
    // Namespace used by locks
    private static final String NAMESPACE = CORFU_SYSTEM_NAMESPACE;
    // Locks table name
    private static final String TABLE_NAME = "LOCK";
    private final Table<LockId, LockData, Message> table;

    private final Uuid clientId;
    // Corfu store to access data from the Lock table.
    private final CorfuStore corfuStore;

    /**
     * Cache of all the observed locks/leases. Contains the last timestamp at which the lock was last observed.
     */
    private final ConcurrentHashMap<LockId, ObservedLock> observedLocks = new ConcurrentHashMap<>();

    /**
     * Constructor
     *
     * @param runtime
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public LockStore(CorfuRuntime runtime, UUID clientUuid) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        this.corfuStore = new CorfuStore(runtime);
        this.table = this.corfuStore.openTable(NAMESPACE,
                TABLE_NAME,
                LockId.class,
                LockData.class,
                null,
                TableOptions.builder().build());

        this.clientId = Uuid.newBuilder()
                .setMsb(clientUuid.getMostSignificantBits())
                .setLsb(clientUuid.getLeastSignificantBits())
                .build();
    }

    /**
     * A client can acquire the lock if:
     * <ul>
     * <li> There is no lock present in the <class>CorfuStore</class></li>
     * <li> The lock in the database is stale.</li>
     * </ul>
     * A lock is considered stale if the lock in database has not been
     * updated for some time (expiration time is configured).
     *
     * @param lockId
     * @return
     * @throws LockStoreException
     */
    public boolean acquire(LockId lockId) throws LockStoreException {
        Optional<LockData> lockInDatastore = get(lockId);

        if (!lockInDatastore.isPresent()) {
            LockData newLockData = LockData.newBuilder()
                    .setLockId(lockId)
                    .setLeaseOwnerId(clientId)
                    .setLeaseRenewalNumber(0)
                    .setLeaseAcquisitionNumber(0)
                    .build();
            // if no lock present acquire(create) the lock in datastore
            create(lockId, newLockData);
            log.debug("Lock: {} Client:{} acquired lock. No pre-existing lease in datastore.", lockId, clientId);
            return true;
        } else {
            if (isRevocable(lockId)) {
                LockData newLockData = LockData.newBuilder()
                        .setLockId(lockId)
                        .setLeaseOwnerId(clientId)
                        .setLeaseRenewalNumber(0)
                        .setLeaseAcquisitionNumber(lockInDatastore.get().getLeaseAcquisitionNumber() + 1)
                        .build();
                // acquire(update) the lock in data store if it is stale
                update(lockId, newLockData);
                log.debug("Lock: {} Client:{} acquired lock {}. Expired lease in datastore: {} ", lockId, clientId, newLockData, lockInDatastore.get());
                return true;
            } else {
                // cannot acquire if some other client holds the lock (non stale)
                log.debug("Lock: {} Client:{} could not acquire lock. Lease in datastore: {}", lockId, clientId, lockInDatastore.get());
                return false;
            }
        }
    }

    /**
     * A client can renew it's lease if it is still the owner of
     * the lease and it has not been revoked by another client.
     *
     * @param lockId
     * @return
     * @throws LockStoreException
     */
    public boolean renew(LockId lockId) throws LockStoreException {
        Optional<LockData> lockInDatastore = get(lockId);

        if (!lockInDatastore.isPresent()) {
            // client had never acquire the lock. This should not happen!
            log.debug("Lock: {} Client:{} could not renew lease. No lock in database.", lockId, clientId);
            return false;
        } else if (!lockInDatastore.get().getLeaseOwnerId().equals(clientId)) {
            // the lease was revoked by another client
            log.debug("Lock: {} Client:{} could not renew lease.Lease in datastore: {}", lockId, clientId, lockInDatastore.get());
            return false;
        } else {
            // renew the lease
            LockData newLockData = LockData.newBuilder()
                    .setLockId(lockId)
                    .setLeaseOwnerId(lockInDatastore.get().getLeaseOwnerId())
                    .setLeaseRenewalNumber(lockInDatastore.get().getLeaseRenewalNumber() + 1)
                    .build();
            update(lockId, newLockData);
            log.debug("Lock: {} Client:{} renewed lease, new lock is {}.", lockId, clientId, newLockData);
            return true;
        }
    }

    /**
     * It checks the <class>LockId</class> passed as input and returns
     * a collection of the ones that have expired leases.
     *
     * @param lockIds
     * @return Collection of lockId(s) that have expired leases.
     * @throws LockStoreException
     */
    public Collection<LockId> filterLocksWithExpiredLeases(Collection<LockId> lockIds) throws LockStoreException {
        Collection<LockId> revocableLeases = new ArrayList<>();
        try {
            // find the leases that can be revoked
            for (LockId lockId: lockIds) {
                if (isRevocable(lockId)) {
                    revocableLeases.add(lockId);
                }
            }
        } catch (Exception e) {
            log.error("Client: {} Exception.", clientId, e);
            throw new LockStoreException("Exception while getting expired leases for client " + clientId, e);
        }
        return revocableLeases;
    }

    /***** HELPER METHODS ******/

    /**
     * Creates a lock record.
     *
     * @param lockId
     * @param lockMetaData
     * @throws LockStoreException
     */
    private void create(LockId lockId, LockData lockMetaData) throws LockStoreException {
        try (TxnContext txnContext = corfuStore.txn(NAMESPACE)) {
            log.info("LockStore: create lock record for : {}", lockId.getLockName());
            txnContext.putRecord(table, lockId, lockMetaData, null);
            txnContext.commit();
        } catch (Exception e) {
            log.error("Lock: {} Exception during create.", lockId, e);
            throw new LockStoreException("Exception while creating lock " + lockId, e);
        }
    }

    /**
     * Updates a lock record.
     *
     * @param lockId
     * @param lockMetaData
     * @throws LockStoreException
     */
    private void update(LockId lockId, LockData lockMetaData) throws LockStoreException {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            txn.putRecord(table, lockId, lockMetaData, null);
            txn.commit();
        } catch (Exception e) {
            log.error("Lock: {} Exception during update.", lockId, e);
            throw new LockStoreException("Exception while updating lock " + lockId, e);
        }
    }

    /**
     * Get a lock record.
     *
     * @param lockId
     * @return
     * @throws LockStoreException
     */
    @VisibleForTesting
    public Optional<LockData> get(LockId lockId) throws LockStoreException {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            CorfuStoreEntry record = txn.getRecord(TABLE_NAME, lockId);
            txn.commit();
            if (record.getPayload() != null) {
                return Optional.of((LockData) record.getPayload());
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            log.error("Lock: {} Exception during get.", lockId, e);
            throw new LockStoreException("Exception while getting data for lock " + lockId, e);
        }
    }

    /**
     * Checks if a lock can be revoked. A lock can be revoked if the client has observed no change
     * in the lock for a given period of time or if there is no lock present in the datastore.
     * @param lockId
     * @return
     * @throws LockStoreException
     */
    private boolean isRevocable(LockId lockId) throws LockStoreException {
        Optional<LockData> lockInDatastore = get(lockId);
        if (lockInDatastore.isPresent()) {
            ObservedLock observedLock = observedLocks.get(lockId);
            if ((observedLock == null) || (!observedLock.lockData.equals(lockInDatastore.get()))) {
                // If the lock has not been observed before or the lock in data store
                // is not the same as the previously observed lock for that key, update the observation
                // lease is not expired yet.
                log.info("LockStore: new observed lock");
                observedLocks.put(lockId, new ObservedLock(lockInDatastore.get(), Instant.now()));
                return false;
            } else {
                // check if the lease has expired
                boolean leaseExpired = observedLock.timestamp.isBefore(Instant.now().minusSeconds(leaseDuration));
                log.info("LockStore: check if lease is expired : {}", leaseExpired);
                if (leaseExpired) {
                    log.debug("LockStore: lock {} lease is expired, leaseDuration={}, timestamp={}, and now={}",
                            lockId, leaseDuration, observedLock.timestamp, Instant.now());
                }
                return leaseExpired;
            }
        } else {
            log.info("LockStore: lockId {} not present in store", lockId.getLockName());
            return true;
        }
    }

    /**
     * Helper class to record a lock/lease when it was last observer.
     * This data is used to determine whether the lock/lease is stale or not.
     */
    @Data
    @AllArgsConstructor
    private class ObservedLock {
        private LockData lockData;
        private Instant timestamp;
    }
}
