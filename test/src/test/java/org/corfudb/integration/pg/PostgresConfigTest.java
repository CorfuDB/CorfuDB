package org.corfudb.integration.pg;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.PgUtils.PostgresConnector;
import org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils;
import org.corfudb.integration.LogReplicationAbstractIT;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.executeQuery;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.tryExecuteCommand;

@Slf4j
public class PostgresConfigTest extends LogReplicationAbstractIT {
    private static final String TEST_PLUGIN_CONFIG_PATH_ACTIVE =
            "./src/test/resources/transport/postgresConfigActive.properties";
    private static final String TEST_PLUGIN_CONFIG_PATH_STANDBY =
            "./src/test/resources/transport/postgresConfigActive.properties";

    private static final Network network = Network.newNetwork();

    private final PostgreSQLContainer<?> pgActive = new PostgreSQLContainer<>("postgres:14")
            .withNetwork(network)
            .withNetworkAliases("postgres1")
            .withCopyFileToContainer(MountableFile.forClasspathResource("pg_hba.conf"), "/postgresql/conf/conf.d/pg_hba.conf")
            .withCommand("postgres -c wal_level=logical -c hba_file=/postgresql/conf/conf.d/pg_hba.conf -c listen_addresses=*");


    private final PostgreSQLContainer<?> pgReplica = new PostgreSQLContainer<>("postgres:14")
            .withNetwork(network)
            .withNetworkAliases("postgres2")
            .withCopyFileToContainer(MountableFile.forClasspathResource("pg_hba.conf"), "/postgresql/conf/conf.d/pg_hba.conf")
            .withCommand("postgres -c wal_level=logical -c hba_file=/postgresql/conf/conf.d/pg_hba.conf -c listen_addresses=*");

    private PostgresConnector primary = null;
    private PostgresConnector replica = null;

    private String originalActiveConfig;
    private String originalStandbyConfig;

    @Before
    public void setup() throws IOException {
        originalActiveConfig = Files.readString(Paths.get(TEST_PLUGIN_CONFIG_PATH_ACTIVE));
        originalStandbyConfig = Files.readString(Paths.get(TEST_PLUGIN_CONFIG_PATH_STANDBY));

        Startables.deepStart(pgActive, pgReplica).join();
        Testcontainers.exposeHostPorts(pgActive.getFirstMappedPort(), pgReplica.getFirstMappedPort());

        String activePgPort = String.valueOf(pgActive.getMappedPort(5432));
        String replicaPgPort = String.valueOf(pgReplica.getMappedPort(5432));

        String localhost = "localhost";
        primary = new PostgresConnector(localhost, activePgPort, pgActive.getUsername(),
                pgActive.getPassword(), pgActive.getDatabaseName());
        replica = new PostgresConnector(localhost, replicaPgPort, pgReplica.getUsername(),
                pgReplica.getPassword(), pgReplica.getDatabaseName());

        addContainerAliasToPlugin(primary, true);
        addContainerAliasToPlugin(replica, false);

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

    private void addContainerAliasToPlugin (PostgresConnector aliasConnector, boolean active) throws IOException {

        String configFilePath = active ? TEST_PLUGIN_CONFIG_PATH_ACTIVE : TEST_PLUGIN_CONFIG_PATH_STANDBY;
        String hostAlias = active ? "postgres1" : "postgres2";

        BufferedWriter writer = new BufferedWriter(new FileWriter(configFilePath));

        writer.write("host_alias=" + hostAlias);
        writer.newLine();
        writer.write("port_alias=" + aliasConnector.PORT);
        writer.newLine();
    }

    @After
    public void cleanup() throws InterruptedException, IOException {
        log.info("HERE");
        log.info("{}", executeQuery("SELECT * FROM pg_hba_file_rules();", primary));
        log.info(pgReplica.getLogs());

        TimeUnit.SECONDS.sleep(2);

        pgActive.stop();
        pgReplica.stop();

        Files.writeString(Paths.get(TEST_PLUGIN_CONFIG_PATH_ACTIVE), originalActiveConfig);
        Files.writeString(Paths.get(TEST_PLUGIN_CONFIG_PATH_STANDBY), originalStandbyConfig);
    }

    private void dropTables(boolean source) {
        String dropT1 = "DROP TABLE t1;";
        String dropT2 = "DROP TABLE t2;";
        PostgresConnector connector = source ? primary : replica;

        PostgresUtils.tryExecuteCommand(dropT1, connector);
        PostgresUtils.tryExecuteCommand(dropT2, connector);
    }

    // TODO (Postgres): need to make tests deterministic i.e. not sleep based
    @Test
    public void testReplication() throws Exception {
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        startActiveLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        startStandbyLogReplicator();

        // Allow for publication / subscription setup
        Thread.sleep(15000);

        int numEntries = 10;
        writeData(numEntries, primary, "t1");

        // Let data propagate
        Thread.sleep(10000);

        Assert.assertEquals(getEntryCount(primary), getEntryCount(replica));
    }

    // TODO (Postgres): Alternative for default cluster manager to mimic site changes for PG

    private void writeData(int numEntries, PostgresConnector connector, String table) {
        int rows = getEntryCount(connector);
        if (rows != -1) {
            for (int i = 0; i < numEntries; i++) {
                rows += 1;
                String addEntryCmd = String.format("INSERT INTO %s (key, value, metadata) VALUES ('key_%s', '{\"value\": %s}', '{\"metadata\": %s}');", table, rows, rows, rows);
                tryExecuteCommand(addEntryCmd, connector);
            }
        }
    }

    private int getEntryCount(PostgresConnector connector) {
        String currentCountQuery = "SELECT COUNT(*) FROM t1;";
        List<Map<String, Object>> result = executeQuery(currentCountQuery, connector);
        if (!result.isEmpty()) {
            return Integer.parseInt(String.valueOf(result.get(0).get("count")));
        } else {
            return -1;
        }
    }
}
