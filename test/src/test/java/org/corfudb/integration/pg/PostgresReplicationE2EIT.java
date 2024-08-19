package org.corfudb.integration.pg;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.executeQuery;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.tryExecuteCommand;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.ACTIVE_CONTAINER_VIRTUAL_HOST;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.ACTIVE_CONTAINER_PHYSICAL_PORT;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.CONFIG_CONTAINER_PHYSICAL_PORT;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.PG_CONTAINER_PHYSICAL_HOST;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.STANDBY_CONTAINER_VIRTUAL_HOST;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.STANDBY_CONTAINER_PHYSICAL_PORT;

@Slf4j
public class PostgresReplicationE2EIT extends LogReplicationAbstractIT {
    private static final String TEST_PLUGIN_CONFIG_PATH_ACTIVE =
            "./test/src/test/resources/transport/postgresConfigActive.properties";
    private static final String TEST_PLUGIN_CONFIG_PATH_STANDBY =
            "./test/src/test/resources/transport/postgresConfigStandby.properties";

    private static final Network network = Network.newNetwork();
    private final PostgreSQLContainer<?> pgActive = new PostgreSQLContainer<>("postgres:14")
            .withNetwork(network)
            .withNetworkAliases(ACTIVE_CONTAINER_VIRTUAL_HOST)
            .withCopyFileToContainer(MountableFile.forClasspathResource("pg_hba.conf"), "/postgresql/conf/conf.d/pg_hba.conf")
            .withCommand("postgres -c wal_level=logical -c hba_file=/postgresql/conf/conf.d/pg_hba.conf -c listen_addresses=*")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    cmd.getHostConfig().withPortBindings(
                            new PortBinding(Ports.Binding.bindPort(ACTIVE_CONTAINER_PHYSICAL_PORT), new ExposedPort(5432))
                    )
            ));
    private final PostgreSQLContainer<?> pgReplica = new PostgreSQLContainer<>("postgres:14")
            .withNetwork(network)
            .withNetworkAliases(STANDBY_CONTAINER_VIRTUAL_HOST)
            .withCopyFileToContainer(MountableFile.forClasspathResource("pg_hba.conf"), "/postgresql/conf/conf.d/pg_hba.conf")
            .withCommand("postgres -c wal_level=logical -c hba_file=/postgresql/conf/conf.d/pg_hba.conf -c listen_addresses=*")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    cmd.getHostConfig().withPortBindings(
                            new PortBinding(Ports.Binding.bindPort(STANDBY_CONTAINER_PHYSICAL_PORT), new ExposedPort(5432))
                    )
            ));
    private final PostgreSQLContainer<?> pgConfig = new PostgreSQLContainer<>("postgres:14")
            .withNetwork(network)
            .withNetworkAliases("postgres_config")
            .withCopyFileToContainer(MountableFile.forClasspathResource("pg_hba.conf"), "/postgresql/conf/conf.d/pg_hba.conf")
            .withCommand("postgres -c wal_level=logical -c hba_file=/postgresql/conf/conf.d/pg_hba.conf -c listen_addresses=*")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    cmd.getHostConfig().withPortBindings(
                            new PortBinding(Ports.Binding.bindPort(CONFIG_CONTAINER_PHYSICAL_PORT), new ExposedPort(5432))
                    )
            ));

    private PostgresConnector primary = null;
    private PostgresConnector replica = null;
    private PostgresConnector config = null;

    @Before
    public void setup() throws IOException {

        Startables.deepStart(pgActive, pgReplica, pgConfig).join();
        Testcontainers.exposeHostPorts(pgActive.getFirstMappedPort(), pgReplica.getFirstMappedPort(), pgConfig.getFirstMappedPort());

        String activePgPort = String.valueOf(pgActive.getMappedPort(5432));
        String replicaPgPort = String.valueOf(pgReplica.getMappedPort(5432));
        String configPgPort = String.valueOf(pgConfig.getMappedPort(5432));

        primary = new PostgresConnector(PG_CONTAINER_PHYSICAL_HOST, activePgPort, pgActive.getUsername(),
                pgActive.getPassword(), pgActive.getDatabaseName());
        replica = new PostgresConnector(PG_CONTAINER_PHYSICAL_HOST, replicaPgPort, pgReplica.getUsername(),
                pgReplica.getPassword(), pgReplica.getDatabaseName());
        config = new PostgresConnector(PG_CONTAINER_PHYSICAL_HOST, configPgPort, pgConfig.getUsername(),
                pgConfig.getPassword(), pgConfig.getDatabaseName());

        // Drop subscriptions
        PostgresUtils.dropAllSubscriptions(primary);
        PostgresUtils.dropAllSubscriptions(replica);

        // Drop publications
        PostgresUtils.dropAllPublications(primary);
        PostgresUtils.dropAllPublications(replica);

        // Drop Tables
        dropTables(true);
        dropTables(false);

        // Set up configuration table
        setupConfigTable();
    }

    private void setupConfigTable() {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS topology_config (id SERIAL PRIMARY KEY, config_key TEXT NOT NULL, config_value TEXT NOT NULL);";
        tryExecuteCommand(createTableSQL, config);
    }

    @After
    public void cleanup() throws InterruptedException, IOException {
        pgActive.stop();
        pgReplica.stop();
    }

    private void dropTables(boolean source) {
        String dropT1 = "DROP TABLE t1;";
        String dropT2 = "DROP TABLE t2;";
        PostgresConnector connector = source ? primary : replica;

        PostgresUtils.tryExecuteCommand(dropT1, connector);
        PostgresUtils.tryExecuteCommand(dropT2, connector);
    }

    @Test
    public void testReplication() throws Exception {
        startActiveLogReplicator(TEST_PLUGIN_CONFIG_PATH_ACTIVE);
        startStandbyLogReplicator(TEST_PLUGIN_CONFIG_PATH_STANDBY);

        while (!isReplicationInProgress(primary)) {
            log.info("Waiting for publications / subscriptions to setup!");
            TimeUnit.MILLISECONDS.sleep(500);
        }

        int numEntries = 10;
        writeData(numEntries, primary, "t1");

        while(getEntryCount(replica) < numEntries) {
            log.info("Waiting for replication.");
            TimeUnit.MILLISECONDS.sleep(500);
        }
        Assert.assertEquals(getEntryCount(primary), getEntryCount(replica));
    }

    @Test
    public void testReplicationWithSwitchover() throws Exception {
        startActiveLogReplicator(TEST_PLUGIN_CONFIG_PATH_ACTIVE);
        startStandbyLogReplicator(TEST_PLUGIN_CONFIG_PATH_STANDBY);

        while (!isReplicationInProgress(primary)) {
            log.info("Waiting for publications / subscriptions to setup!");
            TimeUnit.MILLISECONDS.sleep(500);
        }

        int numEntries = 10;
        writeData(numEntries, primary, "t1");

        while(getEntryCount(replica) < numEntries) {
            log.info("Waiting for replication.");
            TimeUnit.MILLISECONDS.sleep(500);
        }
        Assert.assertEquals(getEntryCount(primary), getEntryCount(replica));

        // Role switch
        insertTopologyChangeEvent("SWITCH");
        while (!isReplicationInProgress(replica)) {
            log.info("Waiting for publications / subscriptions to setup, after switch!");
            TimeUnit.MILLISECONDS.sleep(500);
        }

        // Write more data but now replica is promoted to active
        writeData(numEntries, replica, "t1");

        // Wait for replication from old replica to old primary
        while(getEntryCount(primary) < numEntries * 2) {
            log.info("Waiting for replication after switch.");
            TimeUnit.MILLISECONDS.sleep(500);
        }
        Assert.assertEquals(getEntryCount(replica), getEntryCount(primary));
    }

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

    private boolean isReplicationInProgress(PostgresConnector connector) {
        String currentCountQuery = "SELECT COUNT(*) FROM pg_stat_replication";
        List<Map<String, Object>> result = executeQuery(currentCountQuery, connector);
        if (!result.isEmpty()) {
            return Integer.parseInt(String.valueOf(result.get(0).get("count"))) > 0;
        }
        return false;
    }

    private void insertTopologyChangeEvent(String event) {
        String insertConfigSQL = String.format("INSERT INTO topology_config (config_key, config_value) VALUES ('TOPOLOGY_CHANGE', '%s');", event);
        tryExecuteCommand(insertConfigSQL, config);
    }
}
