package org.corfudb.integration.pg;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.PgUtils.PostgresConnector;
import org.corfudb.integration.LogReplicationAbstractIT;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.executeQuery;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.tryExecuteCommand;

@Slf4j
public class PostgresConfigTest extends LogReplicationAbstractIT {
    private final String nodeIp = "localhost";
    private final String remoteIp = "localhost";
    private final String dbName = "postgres";
    private final String user = "postgres";
    private final String password = "password";
    private final String port = "5432";
    private final String remotePort = "5433";

    private final PostgresConnector primary = new PostgresConnector(nodeIp, port, user, password, dbName);
    private final PostgresConnector replica = new PostgresConnector(remoteIp, remotePort, user, password, dbName);

    private static final String TEST_PLUGIN_CONFIG_PATH_ACTIVE =
            "./test/src/test/resources/transport/postgresConfigActive.properties";
    private static final String TEST_PLUGIN_CONFIG_PATH_STANDBY =
            "./test/src/test/resources/transport/postgresConfigStandby.properties";

    // TODO (Postgres): need to implement way for test to bring up own postgres cluster
    //  and make tests deterministic
    // @Test
    public void testReplication() throws Exception {
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        startActiveLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        startStandbyLogReplicator();

        // Allow for publication / subscription setup
        Thread.sleep(15000);

        int numEntries = 10;
        writeData(numEntries, primary);

        // Let data propagate
        Thread.sleep(10000);

        Assert.assertEquals(getEntryCount(primary), getEntryCount(replica));

        // Wait for switchover
        Thread.sleep(25000);

        // Write more data
         writeData(numEntries, replica);

         // Let data propagate
         Thread.sleep(10000);

         // Check replication, replica is new active
         Assert.assertEquals(getEntryCount(replica), getEntryCount(primary));
    }

    private void writeData(int numEntries, PostgresConnector connector) {
        int rows = getEntryCount(connector);
        if (rows != -1) {
            for (int i = 0; i < numEntries; i++) {
                rows += 1;
                String addEntryCmd = String.format("INSERT INTO t1 (a, b) VALUES (%s, %s);", rows, rows);
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
