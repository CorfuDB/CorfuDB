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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class CorfuReplicationLeaderIT extends AbstractIT {
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

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
            CorfuRuntime activeRuntime =
                    CorfuRuntime.fromParameters(params)
                            .setTransactionLogging(true)
                            .parseConfigurationString(PRIMARY_CORFU_ENDPOINT);

            try {
                activeRuntime = activeRuntime.connect();
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
            } finally {
                activeRuntime.shutdown();
            }

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

    private Tuple<UUID, CorfuInterClusterReplicationServer> runReplicationFuture(int port,
                                                                                 UUID uuid,
                                                                                 String pluginConfigPath,
                                                                                 String logReplicationConfigPath) {
        final int duration = 3000;
        String configuredUUID = UuidUtils.asBase64(uuid);
        Sleep.sleepUninterruptibly(Duration.ofMillis(duration));
        String[] args = new String[]{"-m", "--address=localhost", String.valueOf(port),
                "--plugin=" + pluginConfigPath,
                "--log-replication-config=" + logReplicationConfigPath,
                "--node-id=" + configuredUUID};

        CorfuInterClusterReplicationServer server =
                new CorfuInterClusterReplicationServer(args);

        CompletableFuture.runAsync(server);

        return new Tuple<>(uuid, server);
    }

    private Tuple<UUID, CorfuInterClusterReplicationServer> runReplicationFuture(int port,
                                                                                 String pluginConfigPath,
                                                                                 String logReplicationConfigPath) {
        UUID uuid = UUID.randomUUID();
        return runReplicationFuture(port, uuid, pluginConfigPath, logReplicationConfigPath);
    }

    private void cleanUpResources(List<CorfuRuntime> runtimes,
                                  List<CorfuInterClusterReplicationServer> replicationServers,
                                  List<Process> corfuProcesses) {
        runtimes.forEach(CorfuRuntime::shutdown);
        replicationServers.forEach(CorfuInterClusterReplicationServer::cleanShutdown);
        corfuProcesses.forEach(Process::destroy);
    }

    private void forceLockReleaseAndAcquire(LockClient lockClient,
                                            ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> lockCompetitors,
                                            Duration mainThreadSleepDuration) throws Exception {
        Uuid firstLeaseOwnerId;
        while (true) {
            Optional<LockDataTypes.LockData> currentLockData =
                    lockClient.getCurrentLockData("Log_Replication_Group",
                            "Log_Replication_Lock");
            if (currentLockData.isPresent() && currentLockData.get().hasLeaseOwnerId()) {
                firstLeaseOwnerId = currentLockData.get().getLeaseOwnerId();
                final Uuid id = firstLeaseOwnerId;
                Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> first = lockCompetitors
                        .stream()
                        .filter(tuple -> convertUUID(tuple.first)
                                .equals(id)).findFirst();
                if (first.isPresent()) {

                    Tuple<UUID, CorfuInterClusterReplicationServer> server = first.get();
                    log.info("Found lease holder: {}. Deregestering interest.", firstLeaseOwnerId);
                    server.second.getReplicationDiscoveryService()
                            .deregisterToLogReplicationLock();
                    Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> second = lockCompetitors
                            .stream()
                            .filter(tuple -> !convertUUID(tuple.first).equals(convertUUID(server.first)))
                            .findFirst();
                    if (second.isPresent()) {
                        Tuple<UUID, CorfuInterClusterReplicationServer> server2 = second.get();
                        final int waitTime = 3;
                        log.info("Waiting a few seconds before force acquire.");
                        Sleep.sleepUninterruptibly(Duration.ofSeconds(waitTime));
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
    }

    private void forceLockReleaseAndAcquireFlapping(int numFlaps, LockClient client,
                                                    ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> lockCompetitors,
                                                    Duration mainThreadSleepDuration,
                                                    Duration durationBetweenFlaps) throws Exception {
        int flaps = numFlaps;
        while (flaps > 0) {
            Optional<LockDataTypes.LockData> currentLockData =
                    client.getCurrentLockData("Log_Replication_Group",
                            "Log_Replication_Lock");
            if (currentLockData.isPresent() && currentLockData.get().hasLeaseOwnerId()) {
                Uuid firstLeaseOwnerId = currentLockData.get().getLeaseOwnerId();
                final Uuid id = firstLeaseOwnerId;
                Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> first = lockCompetitors
                        .stream()
                        .filter(tuple -> convertUUID(tuple.first)
                                .equals(id)).findFirst();
                if (first.isPresent()) {
                    Tuple<UUID, CorfuInterClusterReplicationServer> server = first.get();
                    log.info("Found lease holder: {}, {}. Deregestering interest.", firstLeaseOwnerId,
                            server.first);
                    server.second.getReplicationDiscoveryService()
                            .deregisterToLogReplicationLock();
                    Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> second = lockCompetitors
                            .stream()
                            .filter(tuple -> !convertUUID(tuple.first).equals(convertUUID(server.first)))
                            .findFirst();
                    if (second.isPresent()) {
                        final Duration sleepDur = Duration.ofSeconds(3);
                        log.info("Sleeping {} before force acquiring a lock.", sleepDur);
                        Sleep.sleepUninterruptibly(sleepDur);
                        Tuple<UUID, CorfuInterClusterReplicationServer> server2 = second.get();
                        server2.second.getReplicationDiscoveryService()
                                .forceAcquireLogReplicationLock();
                        log.info("{} force-acquired a lock.", convertUUID(server2.first));
                        Sleep.sleepUninterruptibly(durationBetweenFlaps);
                        flaps -= 1;
                    } else {
                        throw new IllegalStateException("There should be at least " +
                                "another primary present.");
                    }
                }
            }
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
        }
    }

    private void forceLockRelease(LockClient lockClient,
                                 ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> lockCompetitors) throws Exception {
        while (true) {
            Optional<LockDataTypes.LockData> currentLockData =
                    lockClient.getCurrentLockData("Log_Replication_Group",
                            "Log_Replication_Lock");
            if (currentLockData.isPresent() && currentLockData.get().hasLeaseOwnerId()) {
                Uuid firstLeaseOwnerId = currentLockData.get().getLeaseOwnerId();
                final Uuid id = firstLeaseOwnerId;
                Optional<Tuple<UUID, CorfuInterClusterReplicationServer>> first = lockCompetitors
                        .stream()
                        .filter(tuple -> convertUUID(tuple.first)
                                .equals(id)).findFirst();
                if (first.isPresent()) {
                    Tuple<UUID, CorfuInterClusterReplicationServer> server = first.get();
                    log.info("Found lease holder: {}. Deregestering interest.", firstLeaseOwnerId);
                    server.second.getReplicationDiscoveryService()
                            .deregisterToLogReplicationLock();
                    break;
                }
            }
        }
    }

    @Test
    public void testLockHolderChangesSourceDuringLogEntrySync() throws Exception {
        final int corfuExitErrorCode = 100;
        exit.expectSystemExitWithStatus(corfuExitErrorCode);

        final int numWriteIterations = 20;
        final Duration waitBetweenWrites = Duration.ofSeconds(2);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);

        String pluginConfigPath = "src/test/resources/topology/two_primaries.properties";
        String configPath = "config.properties";

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                TWO_PRIMARY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                ONE_STANDBY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        CorfuRuntime activeRuntime = CorfuRuntime.fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(PRIMARY_CORFU_ENDPOINT);

        CorfuRuntime standbyRuntime =
                CorfuRuntime.fromParameters(params)
                        .setTransactionLogging(true)
                        .parseConfigurationString(STANDBY_CORFU_ENDPOINT);
        try {
            activeRuntime = activeRuntime.connect();
            standbyRuntime = standbyRuntime.connect();

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

            forceLockReleaseAndAcquire(lockClient, primaries, mainThreadSleepDuration);

            while (replicationTableStandby.size() < numWriteIterations) {
                log.info("Standby table size: {}", replicationTableStandby.size());
                Sleep.sleepUninterruptibly(mainThreadSleepDuration);
            }
            backGroundWriter.join();
        } finally {
            cleanUpResources(
                    ImmutableList.of(activeRuntime, standbyRuntime),
                    Stream.concat(primaries.stream(), standBys.stream()).map(t -> t.second)
                            .collect(ImmutableList.toImmutableList()),
                    ImmutableList.of(corfuPrimary, corfuStandBy));
        }

    }


    @Test
    public void testLockHolderContinuouslyFlappingSource() throws Exception {
        final int corfuExitErrorCode = 100;
        exit.expectSystemExitWithStatus(corfuExitErrorCode);
        final int numWriteIterations = 20;
        final Duration waitBetweenWrites = Duration.ofSeconds(2);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);
        final Duration durationBetweenFlaps = Duration.ofSeconds(10);
        final int flaps = 2;

        String pluginConfigPath = "src/test/resources/topology/two_primaries.properties";
        String configPath = "config.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                TWO_PRIMARY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                ONE_STANDBY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        CorfuRuntime activeRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(PRIMARY_CORFU_ENDPOINT);

        CorfuRuntime standbyRuntime =
                CorfuRuntime.fromParameters(params)
                        .setTransactionLogging(true)
                        .parseConfigurationString(STANDBY_CORFU_ENDPOINT);
        try {

            activeRuntime = activeRuntime.connect();
            standbyRuntime = standbyRuntime.connect();

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

            forceLockReleaseAndAcquireFlapping(flaps, lockClient,
                    primaries, mainThreadSleepDuration, durationBetweenFlaps);

            while (replicationTableStandby.size() < numWriteIterations) {
                log.info("Standby table size: {}", replicationTableStandby.size());
                Sleep.sleepUninterruptibly(mainThreadSleepDuration);
            }

            backGroundWriter.join();
        } finally {
            cleanUpResources(
                    ImmutableList.of(activeRuntime, standbyRuntime),
                    Stream.concat(primaries.stream(), standBys.stream()).map(t -> t.second)
                            .collect(ImmutableList.toImmutableList()),
                    ImmutableList.of(corfuPrimary, corfuStandBy));
        }
    }

    @Test
    public void testLockHolderContinuouslyFlappingDestination() throws Exception {
        final int corfuExitErrorCode = 100;
        exit.expectSystemExitWithStatus(corfuExitErrorCode);
        final int numWriteIterations = 20;
        final Duration waitBetweenWrites = Duration.ofSeconds(2);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);
        final Duration durationBetweenFlaps = Duration.ofSeconds(10);
        final int flaps = 2;
        String pluginConfigPath = "src/test/resources/topology/two_standbys.properties";
        String configPath = "config.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                ONE_PRIMARY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                TWO_STANDBY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        CorfuRuntime activeRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(PRIMARY_CORFU_ENDPOINT);

        CorfuRuntime standbyRuntime =
                CorfuRuntime.fromParameters(params)
                        .setTransactionLogging(true)
                        .parseConfigurationString(STANDBY_CORFU_ENDPOINT);

        try {
            activeRuntime = activeRuntime.connect();
            standbyRuntime = standbyRuntime.connect();
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

            forceLockReleaseAndAcquireFlapping(flaps, lockClient,
                    standBys, mainThreadSleepDuration, durationBetweenFlaps);

            while (replicationTableStandby.size() < numWriteIterations) {
                log.info("Standby table size: {}", replicationTableStandby.size());
                Sleep.sleepUninterruptibly(mainThreadSleepDuration);
            }

            backGroundWriter.join();
        } finally {
            cleanUpResources(
                    ImmutableList.of(activeRuntime, standbyRuntime),
                    Stream.concat(primaries.stream(), standBys.stream()).map(t -> t.second)
                            .collect(ImmutableList.toImmutableList()),
                    ImmutableList.of(corfuPrimary, corfuStandBy));
        }
    }

    @Test
    public void testNoLockHolderOnSource() throws Exception {
        final int corfuExitErrorCode = 100;
        exit.expectSystemExitWithStatus(corfuExitErrorCode);
        final int numWriteIterations = 10;
        final Duration waitBetweenWrites = Duration.ofSeconds(1);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);
        final Duration replicationDuration = Duration.ofSeconds(5);
        String pluginConfigPath =
                "src/test/resources/topology/two_primaries.properties";
        String configPath = "config.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                TWO_PRIMARY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                ONE_STANDBY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        CorfuRuntime activeRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(PRIMARY_CORFU_ENDPOINT);

        CorfuRuntime standbyRuntime =
                CorfuRuntime.fromParameters(params)
                        .setTransactionLogging(true)
                        .parseConfigurationString(STANDBY_CORFU_ENDPOINT);

        try {
            activeRuntime = activeRuntime.connect();
            standbyRuntime = standbyRuntime.connect();

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

            forceLockRelease(lockClient, primaries);

            final int standByTableSizeBeforeWrites = replicationTableStandby.size();
            for (int i = numWriteIterations; i < numWriteIterations + numWriteIterations; i++) {
                log.info("Writer: writing iteration: " + i);
                activeRuntime.getObjectsView().TXBegin();
                replicationTable.put(String.valueOf(i), i);
                activeRuntime.getObjectsView().TXEnd();
                Sleep.sleepUninterruptibly(waitBetweenWrites);
            }

            Sleep.sleepUninterruptibly(replicationDuration);

            assertThat(replicationTableStandby.size()).isNotEqualTo(replicationTable.size());
            assertThat(replicationTableStandby.size()).isEqualTo(standByTableSizeBeforeWrites);
        } finally {
            cleanUpResources(
                    ImmutableList.of(activeRuntime, standbyRuntime),
                    Stream.concat(primaries.stream(), standBys.stream()).map(t -> t.second)
                            .collect(ImmutableList.toImmutableList()),
                    ImmutableList.of(corfuPrimary, corfuStandBy));
        }
    }

    @Test
    public void testLockHolderChangesDestinationDuringLogEntrySync() throws Exception {
        final int corfuExitErrorCode = 100;
        exit.expectSystemExitWithStatus(corfuExitErrorCode);
        final int numWriteIterations = 30;
        final Duration waitBetweenWrites = Duration.ofSeconds(1);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);

        String pluginConfigPath = "src/test/resources/topology/two_standbys.properties";
        String configPath = "config.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                ONE_PRIMARY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                TWO_STANDBY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        CorfuRuntime standbyRuntime =
                CorfuRuntime.fromParameters(params)
                        .setTransactionLogging(true)
                        .parseConfigurationString(STANDBY_CORFU_ENDPOINT);
        try {
            standbyRuntime = standbyRuntime.connect();
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

            forceLockReleaseAndAcquire(lockClient, standBys, mainThreadSleepDuration);

            while (replicationTableStandby.size() < numWriteIterations) {
                log.info("Standby table size: {}", replicationTableStandby.size());
                Sleep.sleepUninterruptibly(mainThreadSleepDuration);
            }
            backGroundWriter.join();
        } finally {
            cleanUpResources(
                    ImmutableList.of(standbyRuntime),
                    Stream.concat(primaries.stream(), standBys.stream()).map(t -> t.second)
                            .collect(ImmutableList.toImmutableList()),
                    ImmutableList.of(corfuPrimary, corfuStandBy));
        }
    }

    @Test
    public void testNoLockHolderOnDestination() throws Exception {
        final int numWriteIterations = 10;
        final Duration waitBetweenWrites = Duration.ofSeconds(1);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);
        final Duration replicationDuration = Duration.ofSeconds(5);
        String pluginConfigPath = "src/test/resources/topology/two_standbys_no_lock_holder.properties";
        String configPath = "config.properties";
        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> primaries =
                ONE_PRIMARY_REPLICATION_SERVER_PORT.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        ImmutableList<Tuple<UUID, CorfuInterClusterReplicationServer>> standBys =
                TWO_STANDBY_REPLICATION_SERVER_PORTS.stream()
                        .map(port -> runReplicationFuture(port, pluginConfigPath, configPath))
                        .collect(ImmutableList.toImmutableList());

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        CorfuRuntime activeRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(PRIMARY_CORFU_ENDPOINT);

        CorfuRuntime standbyRuntime =
                CorfuRuntime.fromParameters(params)
                        .setTransactionLogging(true)
                        .parseConfigurationString(STANDBY_CORFU_ENDPOINT);

        try {
            activeRuntime = activeRuntime.connect();
            standbyRuntime = standbyRuntime.connect();

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

            forceLockRelease(lockClient, standBys);

            final int standByTableSizeBeforeWrites = replicationTableStandby.size();

            for (int i = numWriteIterations; i < numWriteIterations + numWriteIterations; i++) {
                log.info("Writer: writing iteration: " + i);
                activeRuntime.getObjectsView().TXBegin();
                replicationTable.put(String.valueOf(i), i);
                activeRuntime.getObjectsView().TXEnd();
                Sleep.sleepUninterruptibly(waitBetweenWrites);
            }

            Sleep.sleepUninterruptibly(replicationDuration);

            assertThat(replicationTableStandby.size()).isNotEqualTo(replicationTable.size());
            assertThat(replicationTableStandby.size()).isEqualTo(standByTableSizeBeforeWrites);
        } finally {
            cleanUpResources(
                    ImmutableList.of(activeRuntime, standbyRuntime),
                    Stream.concat(primaries.stream(), standBys.stream()).map(t -> t.second)
                            .collect(ImmutableList.toImmutableList()),
                    ImmutableList.of(corfuPrimary, corfuStandBy));
        }
    }
}
