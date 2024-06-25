package org.corfudb.integration.pg;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.PgUtils.PostgresConnector;
import org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.tryExecuteCommand;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class PostgresUtilsTest {
    // localhost:5432 -> Primary
    // localhost:5433 -> Standby

    private final String nodeIp = "localhost";
    private final String remoteIp = "localhost";
    private final String dbName = "postgres";
    private final String user = "postgres";
    private final String password = "password";
    private final String port = "5432";
    private final String remotePort = "5433";

    private final PostgresConnector primary = new PostgresConnector(nodeIp,
            port, user, password, dbName);
    private final PostgresConnector replica = new PostgresConnector(remoteIp,
            remotePort, user, password, dbName);

    @Before
    public void setup() {
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

    // TODO (Postgres): allow test framework to spin up own postgres instances
    // @Test
    public void testReplication() throws Exception{
        Set<String> tablesToReplicate = new HashSet<>();
        tablesToReplicate.add("t1");
        tablesToReplicate.add("t2");

        // Open tables
        openTables(true);
        openTables(false);

        // Create Publications
        PostgresUtils.tryExecuteCommand(PostgresUtils.createPublicationCmd(tablesToReplicate, primary), primary);

        // Create Subscriptions
        tryExecuteCommand( PostgresUtils.createSubscriptionCmd(primary, replica), replica);

        // Generate data
        generateData(true);

        // Allow time for data to sync
        TimeUnit.SECONDS.sleep(2);

        // Validate replication
        assertEquals(getTableEntries(true, "t1"),
                getTableEntries(false, "t1"));

        // Add data on source while replica is down
        generateData(true);

        // Validate replication
        assertEquals(getTableEntries(true, "t1"),
                getTableEntries(false, "t1"));

        // Replication is working, now try switchover
        // Start by stop receiving updates on the standby
        PostgresUtils.dropAllSubscriptions(replica);

        // Remove publications from primary
        PostgresUtils.dropAllPublications(primary);

        // "Clear" tables on the primary
        PostgresUtils.truncateTables(new ArrayList<>(tablesToReplicate), primary);

        // Create publications on replica
        PostgresUtils.tryExecuteCommand(PostgresUtils.createPublicationCmd(tablesToReplicate, replica), replica);

        // Create subscription on primary, "full sync" and start streaming from the replica
        tryExecuteCommand( PostgresUtils.createSubscriptionCmd(replica, primary), primary);

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

    // TODO (Postgres): allow test framework to spin up own postgres instances
    // @Test
    public void testReplicationThreaded() throws Exception {
        Set<String> tablesToReplicate = new HashSet<>();
        tablesToReplicate.add("t1");
        tablesToReplicate.add("t2");

        // Open tables
        openTables(true);
        openTables(false);

        // Create Publications
        PostgresUtils.tryExecuteCommand(PostgresUtils.createPublicationCmd(tablesToReplicate, primary), primary);

        // Create Subscriptions
        tryExecuteCommand( PostgresUtils.createSubscriptionCmd(primary, replica), replica);

        // Generate data
        generateData(true);

        // Allow time for data to sync
        TimeUnit.SECONDS.sleep(2);

        // Validate replication
        assertEquals(getTableEntries(true, "t1"),
                getTableEntries(false, "t1"));

        // Add data on source while replica is down
        generateData(true);

        // Validate replication
        assertEquals(getTableEntries(true, "t1"),
                getTableEntries(false, "t1"));

        Thread replicaSwitch = new Thread(() -> {
            // Replication is working, now try switchover
            // Start by stop receiving updates on the standby
            PostgresUtils.dropAllSubscriptions(replica);

            // Create publications on replica
            PostgresUtils.tryExecuteCommand(PostgresUtils.createPublicationCmd(tablesToReplicate, replica), replica);
        });

        Thread primarySwitch = new Thread(() -> {
            // Remove publications from primary
            PostgresUtils.dropAllPublications(primary);

            // "Clear" tables on the primary
            PostgresUtils.truncateTables(new ArrayList<>(tablesToReplicate), primary);

            // Create subscription on primary, "full sync" and start streaming from the replica
            tryExecuteCommand( PostgresUtils.createSubscriptionCmd(replica, primary), primary);
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



