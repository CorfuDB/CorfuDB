package org.corfudb.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.Tuple;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.Sleep;
import org.corfudb.util.UuidUtils;
import org.corfudb.utils.CommonTypes.Uuid;
import org.corfudb.utils.lock.LockClient;
import org.corfudb.utils.lock.LockConfig;
import org.corfudb.utils.lock.LockDataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class CorfuReplicationLeaderIT extends AbstractIT {
    private static final int PRIMARY_CORFU_PORT = 9000;
    private static final int STANDBY_CORFU_PORT = 9001;

    private static final String PRIMARY_CORFU_ENDPOINT = DEFAULT_HOST + ":" + PRIMARY_CORFU_PORT;
    private static final String STANDBY_CORFU_ENDPOINT = DEFAULT_HOST + ":" + STANDBY_CORFU_PORT;

    private static final ImmutableList<Integer> TWO_PRIMARY_REPLICATION_SERVER_PORTS =
            ImmutableList.of(9010, 9011);

    private static final ImmutableList<Integer> ONE_STANDBY_REPLICATION_SERVER_PORT =
            ImmutableList.of(9020);

    private static final ImmutableList<Integer> ONE_PRIMARY_REPLICATION_SERVER_PORT =
            ImmutableList.of(9010);

    private static final ImmutableList<Integer> TWO_STANDBY_REPLICATION_SERVER_PORTS =
            ImmutableList.of(9020, 9021);

    private static final String REPLICATION_TABLE_NAME = "Table001";

    private Process corfuPrimary;
    private Process corfuStandBy;

    @Before
    public void setUp() throws Exception {
        // Bring corfu servers up.
        corfuPrimary = runServer(PRIMARY_CORFU_PORT, true);
        corfuStandBy = runServer(STANDBY_CORFU_PORT, true);
    }

    @After
    public void tearDown() {
        Stream<Process> corfuProcesses = ImmutableList.of(corfuPrimary, corfuStandBy).stream();
        corfuProcesses.forEach(server -> {
            if (server != null) {
                server.destroy();
            }
        });
    }

    /**
     * This task writes a predefined number of records in the replication tables,
     * and sleeps between each write.
     *
     * @param iterations    Number of iterations.
     * @param sleepDuration Sleep between every iteration.
     * @return A future.
     */
    CompletableFuture<Void> runBackGroundWriter(int iterations, Duration sleepDuration) {
        return CompletableFuture.runAsync(() -> {
            CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build();
            CorfuRuntime activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            activeRuntime.parseConfigurationString(PRIMARY_CORFU_ENDPOINT);
            activeRuntime.connect();

            CorfuTable<String, Integer> replicationTable = activeRuntime.getObjectsView()
                    .build()
                    .setStreamName(REPLICATION_TABLE_NAME)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            for (int i = 0; i < iterations; i++) {
                log.info("Writer: writing iteration: " + i);
                activeRuntime.getObjectsView().TXBegin();
                replicationTable.put(String.valueOf(i), i);
                activeRuntime.getObjectsView().TXEnd();
                Sleep.sleepUninterruptibly(sleepDuration);
            }

            activeRuntime.shutdown();
        });
    }

    /**
     * UUID -> Uuid
     *
     * @param uuid
     * @return
     */
    private Uuid convertUUID(UUID uuid) {
        return Uuid.newBuilder()
                .setMsb(uuid.getMostSignificantBits())
                .setLsb(uuid.getLeastSignificantBits())
                .build();
    }

    private Tuple<UUID, CorfuInterClusterReplicationServer> runReplicationFuture(int port, UUID uuid, String pluginConfigPath) {
        final int duration = 1000;
        String configuredUUID = UuidUtils.asBase64(uuid);
        Sleep.sleepUninterruptibly(Duration.ofMillis(duration));
        String[] args = new String[]{"-m", "--address=localhost", String.valueOf(port),
                "--plugin=" + pluginConfigPath, "--node-id=" + configuredUUID};

        CorfuInterClusterReplicationServer server =
                new CorfuInterClusterReplicationServer(args);

        CompletableFuture.runAsync(server::run);

        return new Tuple<>(uuid, server);
    }

    private Tuple<UUID, CorfuInterClusterReplicationServer> runReplicationFuture(int port,
                                                                                 String pluginConfigPath) {
        UUID uuid = UUID.randomUUID();
        return runReplicationFuture(port, uuid, pluginConfigPath);
    }

    @Test
    public void testLockHolderChangesSourceDuringLogEntrySync() throws Exception {
        final int numWriteIterations = 30;
        final Duration waitBetweenWrites = Duration.ofSeconds(2);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);

        String pluginConfigPath = "src/test/resources/topology/two_primaries.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                TWO_PRIMARY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                ONE_STANDBY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        CorfuRuntime activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        activeRuntime.parseConfigurationString(PRIMARY_CORFU_ENDPOINT);
        activeRuntime.connect();

        CorfuRuntime standbyRuntime = new CorfuRuntime(STANDBY_CORFU_ENDPOINT).connect();

        CorfuTable<String, Integer> replicationTableStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(REPLICATION_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        CompletableFuture<Void> backGroundWriter =
                runBackGroundWriter(numWriteIterations, waitBetweenWrites);
        final int third = 3;
        while (replicationTableStandby.size() < numWriteIterations / third) {
            log.info("Standby table size: {}", replicationTableStandby.size());
            Sleep.sleepUninterruptibly(Duration.ofSeconds(1));
        }

        LockClient lockClient = new LockClient(UUID.randomUUID(), LockConfig.builder().build(),
                activeRuntime);

        Uuid firstLeastOwnerId;
        while (true) {
            Optional<LockDataTypes.LockData> currentLockData =
                    lockClient.getCurrentLockData("Log_Replication_Group",
                            "Log_Replication_Lock");
            if (currentLockData.isPresent() && currentLockData.get().hasLeaseOwnerId()) {
                firstLeastOwnerId = currentLockData.get().getLeaseOwnerId();
                final Uuid id = firstLeastOwnerId;
                Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> first = primaries
                        .stream()
                        .filter(tuple -> convertUUID(tuple.first)
                                .equals(id)).findFirst();
                if (first.isPresent()) {
                    Tuple<UUID, CorfuInterClusterReplicationServer> server = first.get();
                    log.info("Found lease holder: {}. Deregestering interest.", firstLeastOwnerId);
                    server.second.getReplicationDiscoveryService()
                            .deregisterToLogReplicationLock();
                    Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> second = primaries
                            .stream()
                            .filter(tuple -> !convertUUID(tuple.first).equals(convertUUID(server.first)))
                            .findFirst();
                    if (second.isPresent()) {
                        Tuple<UUID, CorfuInterClusterReplicationServer> server2 = second.get();
                        server2.second.getReplicationDiscoveryService()
                                .forceAcquireLogReplicationLock();
                        log.info("{} force-acquired a lock.", convertUUID(server2.first));
                        break;
                    } else {
                        throw new IllegalStateException("There should be at least " +
                                "another primary present.");
                    }
                }
            }
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }

        while (replicationTableStandby.size() < numWriteIterations) {
            log.info("Standby table size: {}", replicationTableStandby.size());
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }
        backGroundWriter.join();
    }


    @Test
    public void lockHolderContinuouslyFlappingSource() throws Exception {
        final int numWriteIterations = 20;
        final Duration waitBetweenWrites = Duration.ofSeconds(2);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);
        final Duration durationBetweenFlaps = Duration.ofSeconds(10);

        String pluginConfigPath = "src/test/resources/topology/two_primaries.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                TWO_PRIMARY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                ONE_STANDBY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        CorfuRuntime activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        activeRuntime.parseConfigurationString(PRIMARY_CORFU_ENDPOINT);
        activeRuntime.connect();

        CorfuRuntime standbyRuntime = new CorfuRuntime(STANDBY_CORFU_ENDPOINT).connect();

        CorfuTable<String, Integer> replicationTableStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(REPLICATION_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        CompletableFuture<Void> backGroundWriter =
                runBackGroundWriter(numWriteIterations, waitBetweenWrites);
        final int third = 3;
        while (replicationTableStandby.size() < numWriteIterations / third) {
            log.info("Standby table size: {}", replicationTableStandby.size());
            Sleep.sleepUninterruptibly(Duration.ofSeconds(1));
        }

        LockClient lockClient = new LockClient(UUID.randomUUID(), LockConfig.builder().build(),
                activeRuntime);

        int tries = 2;
        while (tries > 0) {
            Optional<LockDataTypes.LockData> currentLockData =
                    lockClient.getCurrentLockData("Log_Replication_Group",
                            "Log_Replication_Lock");
            if (currentLockData.isPresent() && currentLockData.get().hasLeaseOwnerId()) {
                Uuid firstLeastOwnerId = currentLockData.get().getLeaseOwnerId();
                final Uuid id = firstLeastOwnerId;
                Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> first = primaries
                        .stream()
                        .filter(tuple -> convertUUID(tuple.first)
                                .equals(id)).findFirst();
                if (first.isPresent()) {
                    Tuple<UUID, CorfuInterClusterReplicationServer> server = first.get();
                    log.info("Found lease holder: {}. Deregestering interest.", firstLeastOwnerId);
                    server.second.getReplicationDiscoveryService()
                            .deregisterToLogReplicationLock();
                    Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> second = primaries
                            .stream()
                            .filter(tuple -> !convertUUID(tuple.first).equals(convertUUID(server.first)))
                            .findFirst();
                    if (second.isPresent()) {
                        Tuple<UUID, CorfuInterClusterReplicationServer> server2 = second.get();
                        server2.second.getReplicationDiscoveryService()
                                .forceAcquireLogReplicationLock();
                        server.second.getReplicationDiscoveryService()
                                .resumeInterestToLockReplicationLock();
                        log.info("{} force-acquired a lock.", convertUUID(server2.first));
                        log.info("Sleeping {} times for {} before switching again.", tries, durationBetweenFlaps);
                        Sleep.sleepUninterruptibly(durationBetweenFlaps);
                        tries -= 1;
                    } else {
                        throw new IllegalStateException("There should be at least " +
                                "another primary present.");
                    }
                }
            }
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }

        while (replicationTableStandby.size() < numWriteIterations) {
            log.info("Standby table size: {}", replicationTableStandby.size());
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }

        backGroundWriter.join();
    }

    @Test
    public void testHolderContinuouslyFlappingDestination() throws Exception{
        final int numWriteIterations = 10;
        final Duration waitBetweenWrites = Duration.ofSeconds(2);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);
        final Duration durationBetweenFlaps = Duration.ofSeconds(10);

        String pluginConfigPath = "src/test/resources/topology/two_standbys.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                ONE_PRIMARY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                TWO_STANDBY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        CorfuRuntime activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        activeRuntime.parseConfigurationString(PRIMARY_CORFU_ENDPOINT);
        activeRuntime.connect();

        CorfuRuntime standbyRuntime = new CorfuRuntime(STANDBY_CORFU_ENDPOINT).connect();

        CorfuTable<String, Integer> replicationTableStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(REPLICATION_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        CompletableFuture<Void> backGroundWriter =
                runBackGroundWriter(numWriteIterations, waitBetweenWrites);
        final int third = 3;
        while (replicationTableStandby.size() < numWriteIterations / third) {
            log.info("Standby table size: {}", replicationTableStandby.size());
            Sleep.sleepUninterruptibly(Duration.ofSeconds(1));
        }

        LockClient lockClient = new LockClient(UUID.randomUUID(), LockConfig.builder().build(),
                standbyRuntime);

        int tries = 2;
        while (tries > 0) {
            Optional<LockDataTypes.LockData> currentLockData =
                    lockClient.getCurrentLockData("Log_Replication_Group",
                            "Log_Replication_Lock");
            if (currentLockData.isPresent() && currentLockData.get().hasLeaseOwnerId()) {
                Uuid firstLeastOwnerId = currentLockData.get().getLeaseOwnerId();
                final Uuid id = firstLeastOwnerId;
                Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> first = standBys
                        .stream()
                        .filter(tuple -> convertUUID(tuple.first)
                                .equals(id)).findFirst();
                if (first.isPresent()) {
                    Tuple<UUID, CorfuInterClusterReplicationServer> server = first.get();
                    log.info("Found lease holder: {}. Deregestering interest.", firstLeastOwnerId);
                    server.second.getReplicationDiscoveryService()
                            .deregisterToLogReplicationLock();
                    Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> second = standBys
                            .stream()
                            .filter(tuple -> !convertUUID(tuple.first).equals(convertUUID(server.first)))
                            .findFirst();
                    if (second.isPresent()) {
                        Tuple<UUID, CorfuInterClusterReplicationServer> server2 = second.get();
                        server2.second.getReplicationDiscoveryService()
                                .forceAcquireLogReplicationLock();
                        server.second.getReplicationDiscoveryService()
                                .resumeInterestToLockReplicationLock();
                        log.info("{} force-acquired a lock.", convertUUID(server2.first));
                        log.info("Sleeping {} times for {} before switching again.", tries, durationBetweenFlaps);
                        Sleep.sleepUninterruptibly(durationBetweenFlaps);
                        tries -= 1;
                    } else {
                        throw new IllegalStateException("There should be at least " +
                                "another primary present.");
                    }
                }
            }
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }

        while (replicationTableStandby.size() < numWriteIterations) {
            log.info("Standby table size: {}", replicationTableStandby.size());
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }

        backGroundWriter.join();
    }

    @Test
    public void testNoLockHolderOnSource() throws Exception {
        final int numWriteIterations = 10;
        final Duration waitBetweenWrites = Duration.ofSeconds(1);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);
        final Duration replicationDuration = Duration.ofSeconds(5);
        String pluginConfigPath = "src/test/resources/topology/two_primaries_no_lock_holder.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                TWO_PRIMARY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                ONE_STANDBY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        CorfuRuntime activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        activeRuntime.parseConfigurationString(PRIMARY_CORFU_ENDPOINT);
        activeRuntime.connect();

        CorfuRuntime standbyRuntime = new CorfuRuntime(STANDBY_CORFU_ENDPOINT).connect();

        CorfuTable<String, Integer> replicationTable = activeRuntime.getObjectsView()
                .build()
                .setStreamName(REPLICATION_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        CorfuTable<String, Integer> replicationTableStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(REPLICATION_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        for (int i = 0; i < numWriteIterations; i++) {
            log.info("Writer: writing iteration: " + i);
            activeRuntime.getObjectsView().TXBegin();
            replicationTable.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
            Sleep.sleepUninterruptibly(waitBetweenWrites);
        }

        while (replicationTableStandby.size() < numWriteIterations) {
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }

        assertThat(replicationTableStandby.size()).isEqualTo(replicationTable.size());

        LockClient lockClient = new LockClient(UUID.randomUUID(), LockConfig.builder().build(),
                activeRuntime);

        Optional<LockDataTypes.LockData> currentLockData =
                lockClient.getCurrentLockData("Log_Replication_Group",
                        "Log_Replication_Lock");

        while (true) {
            if (currentLockData.isPresent() && currentLockData.get().hasLeaseOwnerId()) {
                Uuid firstLeastOwnerId = currentLockData.get().getLeaseOwnerId();
                final Uuid id = firstLeastOwnerId;
                Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> first = primaries
                        .stream()
                        .filter(tuple -> convertUUID(tuple.first)
                                .equals(id)).findFirst();
                if (first.isPresent()) {
                    Tuple<UUID, CorfuInterClusterReplicationServer> server = first.get();
                    log.info("Found lease holder: {}. Deregestering interest.", firstLeastOwnerId);
                    server.second.getReplicationDiscoveryService()
                            .deregisterToLogReplicationLock();
                    break;
                }
            }
        }

        for (int i = numWriteIterations; i < numWriteIterations + numWriteIterations; i++) {
            log.info("Writer: writing iteration: " + i);
            activeRuntime.getObjectsView().TXBegin();
            replicationTable.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
            Sleep.sleepUninterruptibly(waitBetweenWrites);
        }

        Sleep.sleepUninterruptibly(replicationDuration);

        assertThat(replicationTableStandby.size()).isNotEqualTo(replicationTable.size());
    }

    @Test
    public void testLockHolderChangesDestinationDuringLogEntrySync() throws Exception {
        final int numWriteIterations = 30;
        final Duration waitBetweenWrites = Duration.ofSeconds(1);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);

        String pluginConfigPath = "src/test/resources/topology/two_standbys.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                ONE_PRIMARY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                TWO_STANDBY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        CorfuRuntime activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        activeRuntime.parseConfigurationString(PRIMARY_CORFU_ENDPOINT);
        activeRuntime.connect();

        CorfuRuntime standbyRuntime = new CorfuRuntime(STANDBY_CORFU_ENDPOINT).connect();

        CorfuTable<String, Integer> replicationTableStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(REPLICATION_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        CompletableFuture<Void> backGroundWriter =
                runBackGroundWriter(numWriteIterations, waitBetweenWrites);
        final int third = 3;
        while (replicationTableStandby.size() < numWriteIterations / third) {
            log.info("Standby table size: {}", replicationTableStandby.size());
            Sleep.sleepUninterruptibly(Duration.ofSeconds(1));
        }

        LockClient lockClient = new LockClient(UUID.randomUUID(), LockConfig.builder().build(),
                standbyRuntime);

        Uuid firstLeastOwnerId;
        while (true) {
            Optional<LockDataTypes.LockData> currentLockData =
                    lockClient.getCurrentLockData("Log_Replication_Group",
                            "Log_Replication_Lock");
            if (currentLockData.isPresent() && currentLockData.get().hasLeaseOwnerId()) {
                firstLeastOwnerId = currentLockData.get().getLeaseOwnerId();
                final Uuid id = firstLeastOwnerId;
                Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> first = standBys
                        .stream()
                        .filter(tuple -> convertUUID(tuple.first)
                                .equals(id)).findFirst();
                if (first.isPresent()) {
                    Tuple<UUID, CorfuInterClusterReplicationServer> server = first.get();
                    log.info("Found lease holder: {}. Deregestering interest.", firstLeastOwnerId);
                    server.second.getReplicationDiscoveryService()
                            .deregisterToLogReplicationLock();
                    Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> second = standBys
                            .stream()
                            .filter(tuple -> !convertUUID(tuple.first).equals(convertUUID(server.first)))
                            .findFirst();
                    if (second.isPresent()) {
                        Tuple<UUID, CorfuInterClusterReplicationServer> server2 = second.get();
                        server2.second.getReplicationDiscoveryService()
                                .forceAcquireLogReplicationLock();
                        log.info("{} force-acquired a lock.", convertUUID(server2.first));
                        break;
                    } else {
                        throw new IllegalStateException("There should be at least " +
                                "another primary present.");
                    }
                }
            }
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }

        while (replicationTableStandby.size() < numWriteIterations) {
            log.info("Standby table size: {}", replicationTableStandby.size());
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }
        backGroundWriter.join();
    }

    @Test
    public void testNoLockHolderOnDestination() throws Exception {
        final int numWriteIterations = 10;
        final Duration waitBetweenWrites = Duration.ofSeconds(1);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);
        final Duration replicationDuration = Duration.ofSeconds(5);
        String pluginConfigPath = "src/test/resources/topology/two_standbys_no_lock_holder.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                ONE_PRIMARY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                TWO_STANDBY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        CorfuRuntime activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        activeRuntime.parseConfigurationString(PRIMARY_CORFU_ENDPOINT);
        activeRuntime.connect();

        CorfuRuntime standbyRuntime = new CorfuRuntime(STANDBY_CORFU_ENDPOINT).connect();

        CorfuTable<String, Integer> replicationTable = activeRuntime.getObjectsView()
                .build()
                .setStreamName(REPLICATION_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        CorfuTable<String, Integer> replicationTableStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(REPLICATION_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        for (int i = 0; i < numWriteIterations; i++) {
            log.info("Writer: writing iteration: " + i);
            activeRuntime.getObjectsView().TXBegin();
            replicationTable.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
            Sleep.sleepUninterruptibly(waitBetweenWrites);
        }

        while (replicationTableStandby.size() < numWriteIterations) {
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }

        assertThat(replicationTableStandby.size()).isEqualTo(replicationTable.size());

        LockClient lockClient = new LockClient(UUID.randomUUID(), LockConfig.builder().build(),
                standbyRuntime);

        Optional<LockDataTypes.LockData> currentLockData =
                lockClient.getCurrentLockData("Log_Replication_Group",
                        "Log_Replication_Lock");

        while (true) {
            if (currentLockData.isPresent() && currentLockData.get().hasLeaseOwnerId()) {
                Uuid firstLeastOwnerId = currentLockData.get().getLeaseOwnerId();
                final Uuid id = firstLeastOwnerId;
                Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> first = standBys
                        .stream()
                        .filter(tuple -> convertUUID(tuple.first)
                                .equals(id)).findFirst();
                if (first.isPresent()) {
                    Tuple<UUID, CorfuInterClusterReplicationServer> server = first.get();
                    log.info("Found lease holder: {}. Deregestering interest.", firstLeastOwnerId);
                    server.second.getReplicationDiscoveryService()
                            .deregisterToLogReplicationLock();
                    break;
                }
            }
        }

        for (int i = numWriteIterations; i < numWriteIterations + numWriteIterations; i++) {
            log.info("Writer: writing iteration: " + i);
            activeRuntime.getObjectsView().TXBegin();
            replicationTable.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
            Sleep.sleepUninterruptibly(waitBetweenWrites);
        }

        Sleep.sleepUninterruptibly(replicationDuration);

        assertThat(replicationTableStandby.size()).isNotEqualTo(replicationTable.size());
    }


}
