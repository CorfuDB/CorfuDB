package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;


import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.Sleep;
import org.corfudb.utils.TestLockListener;
import org.corfudb.utils.lock.Lock;
import org.corfudb.utils.lock.LockClient;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.LockListener;
import org.corfudb.utils.lock.states.HasLeaseState;
import org.corfudb.utils.lock.states.LockState;
import org.corfudb.utils.lock.states.LockStateType;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

@Slf4j
public class LockIT extends AbstractIT implements Observer {

    private static final int LOCK_TIME_CONSTANT = 6;
    private static final int MONITOR_LOCK_TIME_CONSTANT = 2;
    private static final int LOCK_LEASE_DURATION = 15;
    private final int RENEW_CYCLES = 5;

    private static final String LOCK_GROUP = "Default_Lock_Group";
    private static final String LOCK_NAME = "Test_Lock";

    private Map<UUID, ObservableValue<Integer>> lockAcquiredObservables = new HashMap<>();
    private Map<UUID, ObservableValue<Integer>> lockRevokedObservables = new HashMap<>();
    private Map<UUID, CorfuRuntime> clientIdToRuntimeMap = new HashMap<>();

    // A semaphore that allows to block until the observed value reaches the expected value
    private final Semaphore blockUntilWaitCondition = new Semaphore(1, true);

    private Process corfuServer = null;
    private final int activeSiteCorfuPort = 9000;
    private final String corfuEndpoint = DEFAULT_HOST + ":" + activeSiteCorfuPort;

    private WaitConditionType waitCondition = WaitConditionType.NONE;

    /**
     * Verify that in the case of a single lock client, the client is able to acquire
     * the lock right away, and that it is renewed for several cycles (kept by the client).
     */
    @Test
    public void testSingleLockClient() throws Exception {

        try {
           // Start Single Corfu Node Cluster
           corfuServer = runServer(activeSiteCorfuPort, true);
           initialize();

           // Initial acquisition of the semaphore so we can later block until execution conditions are met
           blockUntilWaitCondition.acquire();

           UUID clientId = UUID.randomUUID();
           LockClient client = createLockClient(clientId);
           LockListener listener = createLockClientListener(clientId);

           client.registerInterest(LOCK_GROUP, LOCK_NAME, listener);

           LockDataTypes.LockId lockId =  LockDataTypes.LockId.newBuilder()
                    .setLockGroup(LOCK_GROUP)
                    .setLockName(LOCK_NAME)
                    .build();

           // Since this is the only client, lock should've been acquired, verify, block until condition is met
           log.debug("***** Wait until lock is acquired");
           waitCondition = WaitConditionType.LOCK_ACQUIRED;
           blockUntilWaitCondition.acquire();

           int lockCount = 1;
           assertThat(lockAcquiredObservables.get(clientId).getValue()).isEqualTo(lockCount);
           while (client.getLocks().get(lockId).getState().getType() != LockStateType.HAS_LEASE) {
               // This is required because we might attempt to retrieve the lock state before it is
               // updated. The lock listener (our blocking condition) is notified previous to updating
               // the new state of the Lock FSM.
           }
           assertThat(client.getLocks().get(lockId).getState().getType()).isEqualTo(LockStateType.HAS_LEASE);
           assertThat(lockRevokedObservables.get(clientId).getValue()).isEqualTo(0);

           waitCondition = WaitConditionType.NONE;

           // Verify for 5 cycles that the lock is renewed
           for (int i=0; i < RENEW_CYCLES; i++) {
               log.debug("***** Wait until lock is renewed");
               // Wait for the renewal cycle + 1, and verify that the lock is still acquired
               Sleep.sleepUninterruptibly(Duration.ofSeconds(LOCK_TIME_CONSTANT + 1));
               assertThat(lockAcquiredObservables.get(clientId).getValue()).isEqualTo(lockCount);
               assertThat(client.getLocks().get(lockId).getState().getType()).isEqualTo(LockStateType.HAS_LEASE);
               assertThat(lockRevokedObservables.get(clientId).getValue()).isEqualTo(0);
           }
       } catch (Exception e) {
           log.debug("Unexpected exception: " + e);
           throw e;
       } finally {
           shutdown();
       }
    }

    /**
     * Verify that when a single client subscribes to multiple (different) locks,
     * it acquires all of them (as no other client is competing for any of them)
     */
    @Test
    public void testSingleLockClientMultipleLocks() throws Exception {

        final int numLocks = 10;

        try {
            // Start Single Corfu Node Cluster
            corfuServer = runServer(activeSiteCorfuPort, true);
            initialize();

            // Initial acquisition of the semaphore so we can later block until execution conditions are met
            blockUntilWaitCondition.acquire();

            UUID clientId = UUID.randomUUID();
            LockClient client = createLockClient(clientId);

            // Lets provide the same listener for all locks
            LockListener listener = createLockClientListener(clientId);

            List<LockDataTypes.LockId> lockIds = new ArrayList<>();
            for (int i=0; i<numLocks; i++) {
                // Locks for same group
                client.registerInterest(LOCK_GROUP, LOCK_NAME + i, listener);
                lockIds.add(LockDataTypes.LockId.newBuilder()
                        .setLockGroup(LOCK_GROUP)
                        .setLockName(LOCK_NAME + i)
                        .build());
            }

            // Since this is the only client, ALL locks should've been acquired, verify, block until condition is met
            waitCondition = WaitConditionType.LOCK_ACQUIRED;
            for (int i=0; i<numLocks; i++) {
                log.debug("***** Wait until lock {} is acquired", i);
                blockUntilWaitCondition.acquire();
            }

            assertThat(lockAcquiredObservables.get(clientId).getValue()).isEqualTo(numLocks);
            lockIds.forEach(lockId -> {
                    while(client.getLocks().get(lockId).getState().getType() != LockStateType.HAS_LEASE) {
                        // Lock state might take longer to update as the listener is updated before the FSM transitions
                        // to the new state
                    }
                    assertThat(client.getLocks().get(lockId).getState().getType()).isEqualTo(LockStateType.HAS_LEASE);
            });
        } catch (Exception e) {
            log.debug("Unexpected exception: " + e);
            throw e;
        } finally {
            shutdown();
        }
    }

    /**
     * Verify that multiple clients can register interest for the same lock,
     * while only one will acquire it, and upon renewal only one has the lease.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleLockClientsSameLock() throws Exception {

        final int numClients = 3;
        Map<UUID, LockClient> clientIdToLockClient = new HashMap<>();
        Map<UUID, LockListener> clientIdToLockListener = new HashMap<>();

        try {
            corfuServer = runServer(activeSiteCorfuPort, true);
            initialize();

            LockDataTypes.LockId lockId =  LockDataTypes.LockId.newBuilder()
                    .setLockGroup(LOCK_GROUP)
                    .setLockName(LOCK_NAME)
                    .build();

            blockUntilWaitCondition.acquire();

            // Initialize 3 Lock Clients, and register to the same lock

            for (int i=0; i<numClients; i++) {
                UUID clientId = UUID.randomUUID();
                LockClient client = createLockClient(clientId);
                LockListener listener = createLockClientListener(clientId);
                clientIdToLockClient.put(clientId, client);
                clientIdToLockListener.put(clientId, listener);
            }

            // Register Interest on common lock for all 3 clients
            ExecutorService executorService = Executors.newFixedThreadPool(numClients);

            Collection<Callable<Boolean>> callableList = new ArrayList<>();
            clientIdToLockClient.forEach( (id, client) -> {
                callableList.add(() -> {
                    client.registerInterest(LOCK_GROUP, LOCK_NAME, clientIdToLockListener.get(id));
                    return true;
                });
            });

            List<Future<Boolean>> futures = executorService.invokeAll(callableList);

            // Wait for all registrations to complete
            for (Future<Boolean> future : futures) {
                if (!future.isDone()) {
                    future.wait();
                }
                assertThat(future.get()).isTrue();
            }

            // Block until any client acquires the lock
            if (lockHasNotBeenAcquired(lockAcquiredObservables)) {
                waitCondition = WaitConditionType.LOCK_ACQUIRED;
                blockUntilWaitCondition.acquire();
            }

            // Verify that only one client has acquired the lock
            // We might need to verify for several cycles as
            // the observable indicating the lock was acquired is
            // triggered before the state update
            List<LockClient> clientsWithLock;
            do {
                clientsWithLock = getClientsThatAcquiredLock(lockId, clientIdToLockClient);
            } while (clientsWithLock == null || clientsWithLock.isEmpty());

            assertThat(clientsWithLock.size()).isEqualTo(1);
            LockClient clientWithLock = clientsWithLock.get(0);

            // Verify for 5 cycles that the lock is renewed
            for (int i=0; i < RENEW_CYCLES; i++) {
                log.debug("***** Wait until lock is renewed");
                // Wait for the renewal cycle + 1, and verify that the lock is still acquired by the same client
                Sleep.sleepUninterruptibly(Duration.ofSeconds(LOCK_TIME_CONSTANT + 1));

                // We might need to verify for several cycles as
                // the observable indicating the lock was acquired is
                // triggered before the state update
                do {
                    clientsWithLock = getClientsThatAcquiredLock(lockId, clientIdToLockClient);
                } while (clientsWithLock == null || clientsWithLock.isEmpty());
                assertThat(clientsWithLock.size()).isEqualTo(1);
                assertThat(clientsWithLock.get(0)).isEqualTo(clientWithLock);
            }

        } catch (Exception e) {
            log.debug("Caught Exception: " + e);
            throw e;
        } finally {
            shutdown();
        }
    }

    /**
     * Verify that multiple clients can register interest for the same lock,
     * and that the lock is acquired by a new client whenever the owner is down.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleLockClientsSameLockFailure() throws Exception {

        final int numClients = 3;
        Map<UUID, LockClient> clientIdToLockClient = new HashMap<>();
        Map<UUID, LockListener> clientIdToLockListener = new HashMap<>();

        try {
            corfuServer = runServer(activeSiteCorfuPort, true);
            initialize();

            LockDataTypes.LockId lockId =  LockDataTypes.LockId.newBuilder()
                    .setLockGroup(LOCK_GROUP)
                    .setLockName(LOCK_NAME)
                    .build();

            blockUntilWaitCondition.acquire();

            // Initialize 3 Lock Clients, and register to the same lock
            for (int i=0; i<numClients; i++) {
                UUID clientId = UUID.randomUUID();
                LockClient client = createLockClient(clientId);
                LockListener listener = createLockClientListener(clientId);
                clientIdToLockClient.put(clientId, client);
                clientIdToLockListener.put(clientId, listener);
            }

            // Register Interest on common lock for all 3 clients
            ExecutorService executorService = Executors.newFixedThreadPool(numClients);

            Collection<Callable<Boolean>> callableList = new ArrayList<>();
            clientIdToLockClient.forEach( (id, client) -> {
                callableList.add(() -> {
                    client.registerInterest(LOCK_GROUP, LOCK_NAME, clientIdToLockListener.get(id));
                    return true;
                });
            });

            List<Future<Boolean>> futures = executorService.invokeAll(callableList);

            // Wait for all registrations to complete
            for (Future<Boolean> future : futures) {
                if (!future.isDone()) {
                    future.wait();
                }
                assertThat(future.get()).isTrue();
            }

            // Block until any client acquires the lock
            if (lockHasNotBeenAcquired(lockAcquiredObservables)) {
                waitCondition = WaitConditionType.LOCK_ACQUIRED;
                blockUntilWaitCondition.acquire();
            }

            // Verify that only one client has acquired the lock
            // We might need to verify for several cycles as
            // the observable indicating the lock was acquired is
            // triggered before the state update
            List<LockClient> clientsWithLock;
            do {
                clientsWithLock = getClientsThatAcquiredLock(lockId, clientIdToLockClient);
            } while (clientsWithLock == null || clientsWithLock.isEmpty());

            assertThat(clientsWithLock.size()).isEqualTo(1);
            LockClient clientWithLock = clientsWithLock.get(0);

            // Verify that if clientWithLock is shutdown, a new client acquires the lock
            clientIdToRuntimeMap.get(clientWithLock.getClientId()).shutdown();

            log.debug("***** Re-acquire lock");

            // Block until any client acquires the lock
            if (lockHasNotBeenAcquired(lockAcquiredObservables)) {
                waitCondition = WaitConditionType.LOCK_ACQUIRED;
                blockUntilWaitCondition.acquire();
            }

            // Verify that only one client has acquired the lock

            // We might need to verify for several cycles as
            // the observable indicating the lock was acquired is
            // triggered before the state update
            do {
                clientsWithLock = getClientsThatAcquiredLock(lockId, clientIdToLockClient);
            } while (clientsWithLock == null || clientsWithLock.isEmpty());

            assertThat(clientsWithLock.size()).isEqualTo(1);
            LockClient newClientWithLock = clientsWithLock.get(0);

            assertThat(newClientWithLock).isNotEqualTo(clientsWithLock);

        } catch (Exception e) {
            log.debug("Caught Exception: " + e);
            throw e;
        } finally {
            shutdown();
        }
    }

    private boolean lockHasNotBeenAcquired(Map<UUID, ObservableValue<Integer>> lockAcquiredObservables) {
        return lockAcquiredObservables.values().stream()
                .allMatch(acquiredObservable -> acquiredObservable.getValue() == 0);
    }

    private List<LockClient> getClientsThatAcquiredLock(LockDataTypes.LockId lockId, Map<UUID, LockClient> idToClient) {
        return idToClient.values().stream()
                .filter(client -> client.getLocks().get(lockId).getState().getType().equals(LockStateType.HAS_LEASE))
                .collect(Collectors.toList());
    }

    private LockClient createLockClient(UUID clientId) throws Exception {
        try {
            CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build();
            CorfuRuntime rt = CorfuRuntime.fromParameters(params).parseConfigurationString(corfuEndpoint).connect();
            clientIdToRuntimeMap.put(clientId, rt);
            return new LockClient(clientId, rt);

        } catch (Exception e) {
            throw e;
        }
    }

    private LockListener createLockClientListener(UUID clientId) {
        LockListener listener = new TestLockListener();
        lockAcquiredObservables.put(clientId, ((TestLockListener) listener).getLockAcquired());
        lockRevokedObservables.put(clientId, ((TestLockListener) listener).getLockRevoked());
        lockAcquiredObservables.forEach((id, o) -> o.addObserver(this));
        lockRevokedObservables.forEach((id, o) -> o.addObserver(this));
        return listener;
    }

    private void initialize() {
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        runtime = CorfuRuntime.fromParameters(params).parseConfigurationString(corfuEndpoint).connect();

        LockState.setDurationBetweenLeaseRenewals(LOCK_TIME_CONSTANT);
        LockState.setMaxTimeForNotificationListenerProcessing(LOCK_TIME_CONSTANT);
        LockClient.setDurationBetweenLockMonitorRuns(MONITOR_LOCK_TIME_CONSTANT);
        HasLeaseState.setDurationBetweenLeaseChecks(MONITOR_LOCK_TIME_CONSTANT);
        Lock.setLeaseDuration(LOCK_LEASE_DURATION);
    }

    private void shutdown() {
        if (runtime != null) {
            runtime.shutdown();
        }

        if (corfuServer != null) {
            corfuServer.destroy();
        }
    }

    /**
     *  Callback for observed values
     */
    @Override
    public void update(Observable o, Object arg) {
        switch (waitCondition) {
            case LOCK_ACQUIRED:
                if (observableOfType(o, lockAcquiredObservables)) {
                    blockUntilWaitCondition.release();
                }
                break;
            case LOCK_REVOKED:
                if (observableOfType(o, lockRevokedObservables)) {
                    blockUntilWaitCondition.release();
                }
                break;
            default:
                    break;
        }
    }

    private boolean observableOfType(Observable observable, Map<UUID, ObservableValue<Integer>> lockObservables) {
        return lockObservables.values().stream().anyMatch( o -> o == observable);
    }

    enum WaitConditionType {
        NONE,
        LOCK_ACQUIRED,
        LOCK_REVOKED
    }
}
