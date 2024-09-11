package org.corfudb.integration.pg;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.PgUtils.PostgresConnector;
import org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.executeQuery;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.tryExecuteCommand;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.ACTIVE_CONTAINER_VIRTUAL_HOST;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.STANDBY_CONTAINER_VIRTUAL_HOST;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class PostgresUtilsTest {
    private static final Network network = Network.newNetwork();

    private final PostgreSQLContainer<?> pgActive = new PostgreSQLContainer<>("postgres:14")
            .withNetwork(network)
            .withNetworkAliases(ACTIVE_CONTAINER_VIRTUAL_HOST)
            .withCopyFileToContainer(MountableFile.forClasspathResource("pg_hba.conf"), "/postgresql/conf/conf.d/pg_hba.conf")
            .withCommand("postgres -c wal_level=logical -c hba_file=/postgresql/conf/conf.d/pg_hba.conf -c listen_addresses=*");


    private final PostgreSQLContainer<?> pgReplica = new PostgreSQLContainer<>("postgres:14")
            .withNetwork(network)
            .withNetworkAliases(STANDBY_CONTAINER_VIRTUAL_HOST)
            .withCopyFileToContainer(MountableFile.forClasspathResource("pg_hba.conf"), "/postgresql/conf/conf.d/pg_hba.conf")
            .withCommand("postgres -c wal_level=logical -c hba_file=/postgresql/conf/conf.d/pg_hba.conf -c listen_addresses=*");

    private PostgresConnector primary = null;
    private PostgresConnector replica = null;
    private PostgresConnector primaryContainer = null;
    private PostgresConnector replicaContainer = null;

    @Before
    public void setup() {

        Startables.deepStart(pgActive, pgReplica).join();
        Testcontainers.exposeHostPorts(pgActive.getFirstMappedPort(), pgReplica.getFirstMappedPort());

        String activePgPort = String.valueOf(pgActive.getMappedPort(5432));
        String replicaPgPort = String.valueOf(pgReplica.getMappedPort(5432));

        String localhost = "localhost";
        primary = new PostgresConnector(localhost, activePgPort, pgActive.getUsername(),
                pgActive.getPassword(), pgActive.getDatabaseName());
        replica = new PostgresConnector(localhost, replicaPgPort, pgReplica.getUsername(),
                pgReplica.getPassword(), pgReplica.getDatabaseName());

        primaryContainer = new PostgresConnector(ACTIVE_CONTAINER_VIRTUAL_HOST, "5432", pgActive.getUsername(),
                pgActive.getPassword(), pgActive.getDatabaseName());
        replicaContainer = new PostgresConnector(STANDBY_CONTAINER_VIRTUAL_HOST, "5432", pgReplica.getUsername(),
                pgReplica.getPassword(), pgReplica.getDatabaseName());

        // Drop subscriptions
        PostgresUtils.dropAllSubscriptions(primary);
        PostgresUtils.dropAllSubscriptions(replica);

        // Drop publications
        PostgresUtils.dropAllPublications(primary);
        PostgresUtils.dropAllPublications(replica);

        // Drop Tables
        dropTables(true);
        dropTables(false);
    }

    @After
    public void cleanup() throws InterruptedException {
        log.info("{}", executeQuery("SELECT * FROM pg_hba_file_rules();", primary));
        log.info(pgReplica.getLogs());

        TimeUnit.SECONDS.sleep(2);

        pgActive.stop();
        pgReplica.stop();
    }

    @Test
    public void testReplication() throws Exception {
        Set<String> tablesToReplicate = new HashSet<>();
        tablesToReplicate.add("t1");
        tablesToReplicate.add("t2");

        // Open tables
        openTables(true);
        openTables(false);

        // Create Publications
        PostgresUtils.tryExecuteCommand(PostgresUtils.createPublicationCmd(tablesToReplicate, primaryContainer), primary);

        // Create Subscriptions
        tryExecuteCommand(PostgresUtils.createSubscriptionCmd(primaryContainer, replicaContainer, primary), replica);

        // Generate data
        generateData(true);

        // Allow time for data to sync
        TimeUnit.SECONDS.sleep(1);

        // Validate replication
        assertEquals(getTableEntries(true, "t1"),
                getTableEntries(false, "t1"));
    }

    @Test
    public void testReplicationThreaded() throws Exception {
        Set<String> tablesToReplicate = new HashSet<>();
        tablesToReplicate.add("t1");
        tablesToReplicate.add("t2");

        // Open tables
        openTables(true);
        openTables(false);

        // Create Publications
        PostgresUtils.tryExecuteCommand(PostgresUtils.createPublicationCmd(tablesToReplicate, primaryContainer), primary);

        // Create Subscriptions
        tryExecuteCommand( PostgresUtils.createSubscriptionCmd(primaryContainer, replicaContainer, primary), replica);

        // Generate data
        generateData(true);

        // Allow time for data to sync
        TimeUnit.SECONDS.sleep(2);

        // Validate replication
        assertEquals(getTableEntries(true, "t1"),
                getTableEntries(false, "t1"));

        Thread replicaSwitch = new Thread(() -> {
            // Replication is working, now try switchover
            // Start by stop receiving updates on the standby
            PostgresUtils.dropAllSubscriptions(replica);

            // Create publications on replica
            PostgresUtils.tryExecuteCommand(PostgresUtils.createPublicationCmd(tablesToReplicate, replicaContainer), replica);
        });

        Thread primarySwitch = new Thread(() -> {
            // Remove publications from primary
            PostgresUtils.dropAllPublications(primary);

            // "Clear" tables on the primary
            PostgresUtils.truncateTables(new ArrayList<>(tablesToReplicate), primary);

            // Create subscription on primary, "full sync" and start streaming from the replica
            tryExecuteCommand(PostgresUtils.createSubscriptionCmd(replicaContainer, primaryContainer, replica), primary);
        });

        replicaSwitch.start();
        primarySwitch.start();

        replicaSwitch.join();
        primarySwitch.join();

        // Generate data
        generateData(false);

        // Allow time for data to sync
        TimeUnit.SECONDS.sleep(2);

        // Validate replication
        assertEquals(getTableEntries(true, "t1"),
                getTableEntries(false, "t1"));

        generateData(false);

        // Allow time for data to sync
        TimeUnit.SECONDS.sleep(2);

        // Validate replication
        assertEquals(getTableEntries(true, "t1"),
                getTableEntries(false, "t1"));
    }

    private void generateData(boolean source) {
        PostgresConnector connector = source ? primary : replica;

        String currentCountQuery = "SELECT COUNT(*) FROM t1;";

        List<Map<String, Object>> result = PostgresUtils.executeQuery(currentCountQuery, connector);
        int rows = 0;

        if (!result.isEmpty()) {
            rows = Integer.parseInt(String.valueOf(result.get(0).get("count")) )+ 1;
        }

        for (int i = 0; i < 10; i++) {
            String addEntryCmd = String.format("INSERT INTO t1 (a, b) VALUES (%s, %s);", rows, rows);
            PostgresUtils.tryExecuteCommand(addEntryCmd, connector);
            rows += 1;
        }
    }

    private List<Map<String, Object>> getTableEntries(boolean source, String tableName) {
        PostgresConnector connector = source ? primary : replica;

        String selectAllQuery = "SELECT * FROM " + tableName + ";";

        return PostgresUtils.executeQuery(selectAllQuery, connector);
    }

    private void openTables(boolean source) {
        String openT1 = "create table t1(a int primary key, b int);";
        String openT2 = "create table t2(a int primary key, b int);";
        PostgresConnector connector = source ? primary : replica;

        PostgresUtils.tryExecuteCommand(openT1, connector);
        PostgresUtils.tryExecuteCommand(openT2, connector);
    }

    private void dropTables(boolean source) {
        String dropT1 = "DROP TABLE t1;";
        String dropT2 = "DROP TABLE t2;";
        PostgresConnector connector = source ? primary : replica;

        PostgresUtils.tryExecuteCommand(dropT1, connector);
        PostgresUtils.tryExecuteCommand(dropT2, connector);
    }
}
