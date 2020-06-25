package org.corfudb.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.Sleep;
import org.corfudb.utils.CommonTypes.Uuid;
import org.corfudb.utils.lock.LockClient;
import org.corfudb.utils.lock.LockDataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.function.Function;
import java.util.stream.Stream;

public class CorfuReplicationLeaderIT extends AbstractIT {
    private static final String SITE_CONFIG_PATH = "corfu_replication_config_many_primaries.properties";
    private static final int PRIMARY_CORFU_PORT = 9000;
    private static final int STANDBY_CORFU_PORT = 9001;
    private static final String PRIMARY_CORFU_ENDPOINT = DEFAULT_HOST + ":" + PRIMARY_CORFU_PORT;
    private static final String STANDBY_CORFU_ENDPOINT = DEFAULT_HOST + ":" + STANDBY_CORFU_PORT;
    private static final String REPLICATION_TABLE_NAME = "Table001";
    private static final ImmutableList<Integer> PRIMARY_REPLICATION_SERVER_PORTS =
            ImmutableList.of(9010, 9011, 9012);
    private static final ImmutableList<Integer> STANDBY_REPLICATION_SERVER_PORTS =
            ImmutableList.of(9020);

    private Process corfuPrimary;
    private Process corfuStandBy;
    private ImmutableList<UUIDProcess> primaryReplicationServers;
    private ImmutableList<UUIDProcess> standByReplicationServers;

    @Getter
    @AllArgsConstructor
    @ToString
    public class UUIDProcess {
        UUID uuid;
        Process process;
    }

    @Before
    public void setUp() throws Exception {
        corfuPrimary = runServer(PRIMARY_CORFU_PORT, true);
        corfuStandBy = runServer(STANDBY_CORFU_PORT, true);
    }

    @After
    public void tearDown() {
        Stream<Process> corfuProcesses = ImmutableList.of(corfuPrimary, corfuStandBy).stream();
        Stream<Process> replicationServerProcesses = Stream.concat(primaryReplicationServers.stream(),
                standByReplicationServers.stream()).map(UUIDProcess::getProcess);

        Stream<Process> allServers = Stream.concat(corfuProcesses, replicationServerProcesses);
        allServers.forEach(server -> {
            if (server != null) {
                server.destroy();
            }
        });
    }

    /**
     * This task writes a predefined number of records in the replication tables,
     * and sleeps between each write.
     * @param iterations Number of iterations.
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
                System.out.println("Writing: " + i);
                replicationTable.put(String.valueOf(i), i);
                Sleep.sleepUninterruptibly(sleepDuration);
            }
            activeRuntime.shutdown();
        });
    }

    private Uuid convertUUID(UUID uuid) {
        return Uuid.newBuilder()
                .setMsb(uuid.getMostSignificantBits())
                .setLsb(uuid.getLeastSignificantBits())
                .build();
    }

    /**
     * Run: - 1 Primary Corfu Server.
     *      - 1 Standby Corfu Server.
     *      - 3 Primary Log Replication Servers.
     *      - 1 Standby Log Replication Server.
     *
     * 1. Over a period of time start producing data to the primary corfu server.
     * 2. Wait until data is replicated half-way.
     * 3. Find the leader of the replication and kill him.
     * 4. Wait until the lock is acquired by another instance.
     * 5. Make sure all the predetermined data is transferred.
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testLeaderChangeDuringSyncOnActiveCluster() throws Exception {
        final int numWriteIterations = 10;
        final Duration waitBetweenWrites1 = Duration.ofMillis(300);
        final Duration waitBetweenWrites2 = Duration.ofSeconds(1);
        final Duration mainThreadSleepDuration = Duration.ofSeconds(1);

        Sleep.sleepUninterruptibly(Duration.ofMillis(30000));

        // Init primary and standby runtimes and replcation tables.
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

        CorfuRuntime standbyRuntime = new CorfuRuntime(STANDBY_CORFU_ENDPOINT).connect();

        CorfuTable<String, Integer> replicationTableStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(REPLICATION_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        assertThat(replicationTable.size()).isEqualTo(replicationTableStandby.size()).isEqualTo(0);

        // Start writing some data to the table.
        CompletableFuture<Void> backGroundWriter =
                runBackGroundWriter(numWriteIterations, waitBetweenWrites1);

        backGroundWriter.join();

        // Wrap a process to extract UUID later and kill the replication server that holds a lock.
        Function<Integer, UUIDProcess> func = port -> {
            try {
                UUID uuid = UUID.randomUUID();
                System.out.println("Uuid: " + convertUUID(uuid));
                Process process = runReplicationServer(port, true, uuid, SITE_CONFIG_PATH);
                return new UUIDProcess(uuid, process);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        };

        // Run three primary replication servers.
        primaryReplicationServers = PRIMARY_REPLICATION_SERVER_PORTS.stream().map(func)
                .collect(ImmutableList.toImmutableList());

        // Run one standby replication server.
        standByReplicationServers = STANDBY_REPLICATION_SERVER_PORTS.stream().map(func)
                .collect(ImmutableList.toImmutableList());

        // This Finishes as a full sync goes through.
        while (replicationTableStandby.size() < numWriteIterations) {
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
            System.out.println("Replication table size is: " + replicationTable.size());
            System.out.println("Replication standby size is: " + replicationTableStandby.size());

        }
//        LockClient lockClient = new LockClient(UUID.randomUUID(), activeRuntime);
//        Optional<LockDataTypes.LockData> currentLockData = lockClient.getCurrentLockData("Log_Replication_Group",
//                "Log_Replication_Lock");

        // Start a background writer.
        backGroundWriter = runBackGroundWriter(numWriteIterations, waitBetweenWrites2);

        // Never finishes.
        while (replicationTableStandby.size() < numWriteIterations * 2) {
            Sleep.sleepUninterruptibly(mainThreadSleepDuration);
            System.out.println("Second Replication table size is: " + replicationTable.size());
            System.out.println("Second Replication standby size is: " + replicationTableStandby.size());
        }

        backGroundWriter.join();
    }
}
