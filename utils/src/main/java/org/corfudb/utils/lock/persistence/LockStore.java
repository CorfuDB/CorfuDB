package org.corfudb.utils.lock.persistence;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.utils.CommonTypes.Uuid;
import org.corfudb.utils.lock.LockDataTypes.LockData;
import org.corfudb.utils.lock.LockDataTypes.LockId;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
    //TODO align with common system namespace in corfu
    //Namespace used by locks
    private static final String namespace = "CORFU_SYSTEM";
    //Locks table name
    private static final String tableName = "LOCK";
    //Corfu store to access data from the Lock table.
    private CorfuStore corfuStore;
    /**
     * Cache of all the observed locks/leases. Contains the last timestamp at which the lock was last observed.
     */
    private Map<LockId, ObservedLock> observedLocks = new ConcurrentHashMap<>();

    /**
     * Constructor
     *
     * @param runtime
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public LockStore(CorfuRuntime runtime) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        corfuStore = new CorfuStore(runtime);
        corfuStore.openTable(namespace,
                tableName,
                LockId.class,
                LockData.class,
                null,
                TableOptions.builder().build());
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
     * @param clientUuid
     * @return
     * @throws LockStoreException
     */
    public boolean acquire(LockId lockId, UUID clientUuid) throws LockStoreException {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        Optional<LockData> lockInDatastore = get(lockId, timestamp);

        if (!lockInDatastore.isPresent()) {
            Uuid leaseOwnerId = Uuid.newBuilder()
                    .setMsb(clientUuid.getMostSignificantBits())
                    .setLsb(clientUuid.getLeastSignificantBits())
                    .build();
            LockData newLockData = LockData.newBuilder()
                    .setLockId(lockId)
                    .setLeaseOwnerId(leaseOwnerId)
                    .setLeaseRenewalNumber(0)
                    .setLeaseAcquisitionNumber(0)
                    .build();
            // if no lock present acquire(create) the lock in datastore
            create(lockId, newLockData, timestamp);
            log.debug("Lock: {} Client:{} acquired lock. No pre-existing lease in datastore.", lockId, clientUuid);
            return true;
        } else {
            // can acquire lock if the lock is stale
            ObservedLock observedLock = observedLocks.get(lockId);
            if (lockInDatastore.get().equals(observedLock) && observedLock.timestamp.isBefore(Instant.now().minusSeconds(300))) {
                Uuid leaseOwnerId = Uuid.newBuilder()
                        .setMsb(clientUuid.getMostSignificantBits())
                        .setLsb(clientUuid.getLeastSignificantBits())
                        .build();
                LockData newLockData = LockData.newBuilder()
                        .setLockId(lockId)
                        .setLeaseOwnerId(leaseOwnerId)
                        .setLeaseRenewalNumber(0)
                        .setLeaseAcquisitionNumber(lockInDatastore.get().getLeaseAcquisitionNumber() + 1)
                        .build();
                // acquire(update) the lock in data store if it is stale
                update(lockId, newLockData, timestamp);
                log.debug("Lock: {} Client:{} acquired lock. Expired lease in datastore: {} ", lockId, clientUuid, lockInDatastore.get());
                return true;
            } else {
                // cannot acquire if some other client holds the lock (non stale)
                log.debug("Lock: {} Client:{} could not acquire lock. Lease in datastore: {}", lockId, clientUuid, lockInDatastore.get());
                return false;
            }
        }
    }

    /**
     * A client can renew it's lease if it is still the owner of
     * the lease and it has not been revoked by another client.
     *
     * @param lockId
     * @param clientUuid
     * @return
     * @throws LockStoreException
     */
    public boolean renew(LockId lockId, UUID clientUuid) throws LockStoreException {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        Optional<LockData> lockInDatastore = get(lockId, timestamp);

        Uuid leaseOwnerId = Uuid.newBuilder()
                .setMsb(clientUuid.getMostSignificantBits())
                .setLsb(clientUuid.getLeastSignificantBits())
                .build();

        if (!lockInDatastore.isPresent()) {
            // client had never acquire the lock. This should not happen!
            log.debug("Lock: {} Client:{} could not renew lease. No lock in database.", lockId, clientUuid);
            return false;
        } else if (!lockInDatastore.get().getLeaseOwnerId().equals(leaseOwnerId)) {
            // the lease was revoked by another client
            log.debug("Lock: {} Client:{} could not renew lease.Lease in datastore: {}", lockId, clientUuid, lockInDatastore.get());
            return false;
        } else {
            // renew the lease
            LockData newLockData = LockData.newBuilder()
                    .setLockId(lockId)
                    .setLeaseOwnerId(lockInDatastore.get().getLeaseOwnerId())
                    .setLeaseRenewalNumber(lockInDatastore.get().getLeaseRenewalNumber() + 1)
                    .build();
            update(lockId, newLockData, timestamp);
            log.debug("Lock: {} Client:{} renewed lease.", lockId, clientUuid);
            return true;
        }
    }

    /**
     * Creates a lock record.
     *
     * @param lockId
     * @param lockMetaData
     * @param timestamp    Logical time at which to run tx for create
     * @throws LockStoreException
     */
    private void create(LockId lockId, LockData lockMetaData, CorfuStoreMetadata.Timestamp timestamp) throws LockStoreException {
        try {
            corfuStore.tx(namespace)
                    .create(tableName,
                            lockId,
                            lockMetaData,
                            null)
                    .commit(timestamp);
        } catch (Exception e) {
            log.error("Lock: {} Exception during create.", lockId, e);
            throw new LockStoreException("Exception while creating lock " + lockId, e);
        }
    }

    /***** HELPER METHODS ******/

    /**
     * Updates a lock record.
     *
     * @param lockId
     * @param lockMetaData
     * @param timestamp    Logical time at which to run tx for update
     * @throws LockStoreException
     */
    private void update(LockId lockId, LockData lockMetaData, CorfuStoreMetadata.Timestamp timestamp) throws LockStoreException {
        try {
            corfuStore.tx(namespace)
                    .update(tableName,
                            lockId,
                            lockMetaData,
                            null)
                    .commit(timestamp);
        } catch (Exception e) {
            log.error("Lock: {} Exception during update.", lockId, e);
            throw new LockStoreException("Exception while updating lock " + lockId, e);
        }
    }

    /**
     * Get a lock record. It also updates the observedLocks cache.
     *
     * @param lockId
     * @param timestamp Logical time at which to run the query
     * @return
     * @throws LockStoreException
     */
    private Optional<LockData> get(LockId lockId, CorfuStoreMetadata.Timestamp timestamp) throws LockStoreException {
        try {
            CorfuRecord record = corfuStore.query(namespace).getRecord(tableName, timestamp, lockId);
            if (record != null) {
                LockData lockInDatastore = (LockData) record.getPayload();
                //Update the observedLocks Cache.
                ObservedLock observedLock = observedLocks.get(lockId);
                // If the lock has not been observed before or the lock in data store
                // is not the same as the previously observed lock just update the observation, cannot acquire.
                if ((observedLock == null) || (!observedLock.lockData.equals(lockInDatastore))) {
                    // update observation map if the instance of lock has not been observed before.
                    observedLocks.put(lockId, new ObservedLock(lockInDatastore, Instant.now()));
                }
                return Optional.of(lockInDatastore);
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            log.error("Lock: {} Exception during get.", lockId, e);
            throw new LockStoreException("Exception while getting data for lock " + lockId, e);
        }
    }

    /**
     * Helper class to record a lock/lease when it was last observer.
     * This data is used to determine whether the lock/lease is stale or not.
     */
    @Data
    @AllArgsConstructor
    private class ObservedLock {
        LockData lockData;
        Instant timestamp;
    }
}
