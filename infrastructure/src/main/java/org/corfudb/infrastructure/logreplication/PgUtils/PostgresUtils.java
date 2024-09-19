package org.corfudb.infrastructure.logreplication.PgUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.protobuf.Timestamp;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal.SyncType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SnapshotSyncInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncStatus;

import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.ACTIVE_CONTAINER_VIRTUAL_HOST;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.ACTIVE_CONTAINER_PHYSICAL_PORT;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.TEST_PG_DATABASE;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.PG_CONTAINER_PHYSICAL_HOST;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.TEST_PG_PASSWORD;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.TEST_PG_USER;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.STANDBY_CONTAINER_PHYSICAL_PORT;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.isTestEnvironment;

@Slf4j
public class PostgresUtils {

    private static final long RETRY_DELAY_MS = 5000;

    private PostgresUtils() {}

    @Setter
    private static PostgresConnector testClusterConnector = null;

    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    public static <T> T retryIndefinitely(Supplier<T> operation, String operationName) {
        int attempts = 0;
        while (true) {
            try {
                T result = operation.get();
                if (attempts > 0) {
                    log.info("{} succeeded after {} attempts", operationName, attempts + 1);
                }
                return result;
            } catch (Exception e) {
                attempts++;
                log.warn("{} failed. Attempt {}. Error: {}. Retrying in {} ms...",
                        operationName, attempts, e.getMessage(), RETRY_DELAY_MS);
                try {
                    TimeUnit.MILLISECONDS.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    throw new RuntimeException("Interrupted during retry delay", ie);
                }
            }
        }
    }

    public static boolean tryExecuteCommand(String sql, PostgresConnector connector) {
        return tryExecuteCommand(sql, connector, isTestEnvironment);
    }

    public static boolean tryExecutePreparedStatementsCommand(String sql, Object[] params, PostgresConnector connector) {
        return tryExecutePreparedStatementsCommand(sql, params, connector, isTestEnvironment);
    }

    public static List<Map<String, Object>> executeQuery(String sql, PostgresConnector connector) {
        return executeQuery(sql, connector, isTestEnvironment);
    }

    public static List<Map<String, Object>> executePreparedStatementQuery(String sql, Object[] params, PostgresConnector connector) {
        return executePreparedStatementQuery(sql, params, connector, isTestEnvironment);
    }

    public static boolean tryExecuteCommand(String sql, PostgresConnector connector, boolean useContainerConnection) {
        if (useContainerConnection) {
            connector = testClusterConnector;
        }

        boolean successOrExists = false;
        log.info("Executing command: {}, on connector {} ", sql, connector);
        if (!sql.isEmpty()) {
            try (Connection conn = DriverManager.getConnection(connector.url, connector.user, connector.password)) {
                Statement statement = conn.createStatement();
                statement.execute(sql);
                statement.close();
                successOrExists = true;
            } catch (SQLException e) {
                if ("42710".equals(e.getSQLState())) {
                    log.info("Object already exists!!!");
                    successOrExists = true;
                } else if ("42704".equals(e.getSQLState())) {
                    log.info("Object is undefined!!!");
                } else if ("42P07".equals(e.getSQLState())) {
                    log.info("Table already exists!!!");
                    successOrExists = true;
                } else {
                    log.error("Encountered error in executing command.", e);
                }
            }
        }
        return successOrExists;
    }

    public static boolean tryExecutePreparedStatementsCommand(String sql, Object[] params, PostgresConnector connector, boolean useContainerConnection) {
        if (useContainerConnection) {
            connector = testClusterConnector;
        }

        boolean successOrExists = false;
        if (!sql.isEmpty()) {
            try (Connection conn = DriverManager.getConnection(connector.url, connector.user, connector.password)) {
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    setParameters(pstmt, params);
                    log.info("tryExecutePreparedStatementsCommand: Executing command: {}", pstmt.toString());
                    pstmt.executeUpdate();
                    successOrExists = true;
                }
            } catch (SQLException e) {
                if ("42710".equals(e.getSQLState())) {
                    log.info("tryExecutePreparedStatementsCommand: Object already exists!");
                    successOrExists = true;
                } else if ("42704".equals(e.getSQLState())) {
                    log.info("tryExecutePreparedStatementsCommand: Object is undefined!");
                } else if ("42P07".equals(e.getSQLState())) {
                    log.info("tryExecutePreparedStatementsCommand: Table already exists!");
                    successOrExists = true;
                } else {
                    log.info("Encountered error in executing prepared statement command.", e);
                }
            }
        }
        return successOrExists;
    }

    public static void setParameters(PreparedStatement pstmt, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            if (params[i] instanceof String) {
                pstmt.setString(i + 1, (String) params[i]);
            } else if (params[i] instanceof Integer) {
                pstmt.setInt(i + 1, (Integer) params[i]);
            } else if (params[i] instanceof Double) {
                pstmt.setDouble(i + 1, (Double) params[i]);
            } else if (params[i] instanceof Boolean) {
                pstmt.setBoolean(i + 1, (Boolean) params[i]);
            } else if (params[i] instanceof java.sql.Date) {
                pstmt.setDate(i + 1, (java.sql.Date) params[i]);
            } else if (params[i] instanceof java.sql.Timestamp) {
                pstmt.setTimestamp(i + 1, (java.sql.Timestamp) params[i]);
            } else {
                pstmt.setObject(i + 1, params[i]);
            }
        }
    }

    public static List<Map<String, Object>> executeQuery(String sql, PostgresConnector connector, boolean useContainerConnection) {
        if (useContainerConnection) {
            if (testClusterConnector == null) {
                log.warn("Test cluster connector not initialized, invalid usage, continuing with supplied connector!");
            } else{
                connector = testClusterConnector;
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();
        log.info("Executing command: {}, on connector {} ", sql, connector);

        try (Connection conn = DriverManager.getConnection(connector.url, connector.user, connector.password)) {
            Statement statement = conn.createStatement();
            ResultSet results = statement.executeQuery(sql);
            ResultSetMetaData metaData = results.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (results.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), results.getObject(i));
                }
                result.add(row);
            }

            statement.close();
        } catch (SQLException e) {
            log.info("Encountered error while querying.", e);
        }
        return result;
    }

    public static List<Map<String, Object>> executePreparedStatementQuery(String sql, Object[] params, PostgresConnector connector, boolean useContainerConnection) {
        if (useContainerConnection) {
            if (testClusterConnector == null) {
                log.warn("Test cluster connector not initialized, invalid usage, continuing with supplied connector!");
            } else{
                connector = testClusterConnector;
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();
        log.info("Executing query: {}, on connector {} ", sql, connector);

        try (Connection conn = DriverManager.getConnection(connector.url, connector.user, connector.password)) {
            ResultSet results;
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                setParameters(pstmt, params);
                log.info("tryExecutePreparedStatementsCommand: Executing query: {}", pstmt.toString());
                results = pstmt.executeQuery();
                ResultSetMetaData metaData = results.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (results.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(metaData.getColumnName(i), results.getObject(i));
                    }
                    result.add(row);
                }
            } catch (SQLException e) {
                log.error("Encountered error while executing prepared statement query.", e);
                throw e;
            }

        } catch (Exception e) {
            log.info("Encountered error while querying.", e);
        }
        return result;
    }

    public static List<String> createTablesCmds(Set<String> tablesToReplicate) {
        List<String> createTableCmds = new ArrayList<>();

        tablesToReplicate.forEach(table -> {
            String createCmd = String.format("CREATE TABLE IF NOT EXISTS %s (", table) +
                    " key VARCHAR PRIMARY KEY," +
                    " value JSONB NOT NULL," +
                    " metadata JSONB NOT NULL" +
                    " );";

            createTableCmds.add(createCmd);
        });

        return createTableCmds;
    }

    public static String createPublicationCmd(Set<String> tablesToReplicate, PostgresConnector connector) {
        return "CREATE PUBLICATION \"" +
                String.join("_", connector.address.split("\\.")) +
                "_pub\"" +
                " FOR TABLE " +
                String.join(", ", tablesToReplicate) +
                ";";
    }

    public static String createSubscriptionCmd(PostgresConnector primary, PostgresConnector replica) {
        PostgresConnector containerToQuery = primary;
        if (isTestEnvironment) {
            if (Objects.equals(primary.address, ACTIVE_CONTAINER_VIRTUAL_HOST)) {
                containerToQuery = new PostgresConnector(PG_CONTAINER_PHYSICAL_HOST,
                        String.valueOf(ACTIVE_CONTAINER_PHYSICAL_PORT), TEST_PG_USER, TEST_PG_PASSWORD, TEST_PG_DATABASE);
            } else {
                containerToQuery = new PostgresConnector(PG_CONTAINER_PHYSICAL_HOST,
                        String.valueOf(STANDBY_CONTAINER_PHYSICAL_PORT), TEST_PG_USER, TEST_PG_PASSWORD, TEST_PG_DATABASE);
            }
        }
        return createSubscriptionCmd(primary, replica, containerToQuery);
    }

    public static String createSubscriptionCmd(PostgresConnector primary, PostgresConnector replica, PostgresConnector primaryToQuery) {
        String createSubCmd = "";

        if (Objects.equals(primary.address + primary.port, replica.address + replica.port)) {
            log.error("Skipping subscribing to self {}. This is an invalid state!", primary.address);
        } else {
            int max_retry = 10;
            String primaryPrefix = String.join("_", primary.address.split("\\."));
            String replicaPrefix = String.join("_", replica.address.split("\\."));
            String pubName = String.join("_", primaryPrefix, "pub");

            for (int i = 0; i < max_retry; i++) {
                String pubExistsQuery = String.format("SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '%s');", pubName);
                log.info("check exists: {}", pubExistsQuery);
                try {
                    List<Map<String, Object>> queryResult = executeQuery(pubExistsQuery, primaryToQuery, false);

                    if (!queryResult.isEmpty()) {
                        boolean publicationExists = (boolean) queryResult.get(0).values().stream().findAny().get();
                        if (publicationExists) {
                            String subName = String.join("_", replicaPrefix, "sub");
                            createSubCmd = String.format("CREATE SUBSCRIPTION \"%s\" CONNECTION 'host=%s port=%s user=%s dbname=%s password=%s' PUBLICATION \"%s\";",
                                    subName, primary.address, primary.port, primary.user, primary.databaseName, primary.password, pubName);
                            break;
                        } else {
                            log.info("PUB WITH THAT NAME DOES NOT EXIST");
                            TimeUnit.SECONDS.sleep(5);
                        }
                    } else {
                        log.info("PUB DOES NOT EXIST");
                        TimeUnit.SECONDS.sleep(5);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        log.info("CREATE_SUB_CMD: {}", createSubCmd);

        return createSubCmd;
    }

    public static List<String> getAllSubscriptions(PostgresConnector connector) {
        String getSubQuery = "SELECT * FROM pg_subscription;";

        return executeQuery(getSubQuery, connector).stream().map(row -> {
                String subname = row.get("subname").toString();
                if (subname != null && !subname.isEmpty()) {
                    return row.get("subname").toString();
                }
                return null;
            }
        ).filter(Objects::nonNull).collect(Collectors.toList());
    }

    public static void dropSubscriptions(List<String> subscriptionsToDrop, PostgresConnector connector) {
        // TODO (Postgres): Can also decouple from slot to guarantee drop and have service
        //  to clean up inactive slots

        for (String subscription : subscriptionsToDrop) {
            String[] params = {};
            String dropQuery = String.format("DROP SUBSCRIPTION %s", quoteIdentifier(subscription));
            retryIndefinitely(() -> tryExecutePreparedStatementsCommand(dropQuery, params, connector),
                    String.format("Drop for subscription [%s]", subscription));
        }
    }

    public static void clearTables(List<String> tablesToTruncate, PostgresConnector connector) {
        String truncatePrefix = "DELETE FROM ";
        for (String table : tablesToTruncate) {
            retryIndefinitely(() -> tryExecuteCommand(truncatePrefix + table + ";", connector),
                    String.format("Delete from table for table [%s]", table));
        }
    }

    public static void makeTablesReadOnly(List<String> readOnlyTables, PostgresConnector connector) {
        String readOnlySql = "REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLE %s FROM %s;";
        String getRoleNamesQuery = "SELECT rolname FROM pg_roles WHERE rolname !~ 'postgres' AND rolname !~ '^pg';";
        List<Map<String, Object>> rolenamesResult = executeQuery(getRoleNamesQuery, connector);

        for (Map<String, Object> row : rolenamesResult) {
            String roleName = row.get("rolname").toString();
            for (String table : readOnlyTables) {
                retryIndefinitely(() -> tryExecuteCommand(String.format(readOnlySql, table, roleName), connector),
                        String.format("Make table readonly for table [%s]", table));
            }
        }
    }

    public static void makeTablesWriteable(List<String> writeableTables, PostgresConnector connector) {
        String writeableSql = "GRANT INSERT, UPDATE, DELETE, TRUNCATE ON TABLE %s TO %s;";
        String getRoleNamesQuery = "SELECT rolname FROM pg_roles WHERE rolname !~ 'postgres' AND rolname !~ '^pg';";
        List<Map<String, Object>> rolenamesResult = executeQuery(getRoleNamesQuery, connector);

        for (Map<String, Object> row : rolenamesResult) {
            String roleName = row.get("rolname").toString();
            for (String table : writeableTables) {
                retryIndefinitely(() -> tryExecuteCommand(String.format(writeableSql, table, roleName), connector),
                        String.format("Make table writeable for table [%s]", table));
            }
        }
    }

    public static List<String> getAllPublications(PostgresConnector connector) {
        List<String> publicationNames = new ArrayList<>();
        String getSubQuery = "SELECT * FROM pg_publication;";

        for (Map<String, Object> row : executeQuery(getSubQuery, connector)) {
            String pubname = row.get("pubname").toString();

            // TODO (Postgres): filtering out non replication pubs manually, can filter for just replication
            // pubs once naming is standardized.
            if (pubname != null && !pubname.isEmpty() && !pubname.contains("dbz")) {
                publicationNames.add(pubname);
            }
        }

        return publicationNames;
    }

    public static void dropPublications(List<String> publicationsToDrop, PostgresConnector connector) {
        String dropPrefix = "DROP PUBLICATION ";
        for (String publication : publicationsToDrop) {
            retryIndefinitely(() -> tryExecuteCommand(dropPrefix + "\"" +  publication + "\";", connector),
                    String.format("Drop for publication [%s]", publication));
        }
    }

    public static void dropAllSubscriptions(PostgresConnector connector) {
        dropSubscriptions(getAllSubscriptions(connector), connector);
    }

    public static void dropAllPublications(PostgresConnector connector) {
        dropPublications(getAllPublications(connector), connector);
    }

    public static ReplicationStatusVal getPgReplicationStatus(PostgresConnector connector, String remoteHost) {
        // TODO (Postgres): update protobufs to be relevant for pg stats
        String subPostfix = "_sub";
        remoteHost = String.join("_", remoteHost.split("\\."));
        String replicationStatsQuery = "SELECT replay_lsn, flush_lsn, write_lsn, state, " +
                "pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as bytes_lag " +
                "FROM pg_stat_replication " +
                "WHERE application_name = ?;";

        String applicationName = remoteHost + subPostfix;
        Object[] params = new Object[]{applicationName};
        List<Map<String, Object>> pgStatusQuery = executePreparedStatementQuery(replicationStatsQuery, params, connector);
        if (pgStatusQuery.isEmpty()) {
            log.warn("Replication stats are not available!");
            return ReplicationStatusVal.getDefaultInstance();
        }
        Map<String, Object> pgStatus = pgStatusQuery.get(0);

        boolean isDataConsistent = pgStatus.get("flush_lsn").equals(pgStatus.get("write_lsn"));

        String flushLsn = pgStatus.get("flush_lsn").toString();
        flushLsn = flushLsn.substring(flushLsn.indexOf('/') + 1);
        long flushLsnValue = Long.parseLong(flushLsn, 16);

        String replayLsn = pgStatus.get("replay_lsn").toString();
        replayLsn = replayLsn.substring(replayLsn.indexOf('/') + 1);
        long replayLsnValue = Long.parseLong(replayLsn, 16);

        long writeLag = Integer.parseInt(pgStatus.get("bytes_lag").toString());

        SyncStatus syncStatus;
        switch (pgStatus.get("state").toString()) {
            case "startup":
            case "stopping":
                syncStatus = SyncStatus.NOT_STARTED;
                break;

            case "catchup":
            case "streaming":
            case "backup":
                syncStatus = SyncStatus.ONGOING;
                break;

            default:
                syncStatus = SyncStatus.UNRECOGNIZED;
                break;
        }

        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();
        SnapshotSyncInfo syncInfo = SnapshotSyncInfo.newBuilder()
                .setSnapshotRequestId(String.valueOf(replayLsnValue))
                .setCompletedTime(timestamp)
                .setBaseSnapshot(flushLsnValue)
                .setStatus(SyncStatus.COMPLETED)
                .setType(SnapshotSyncType.DEFAULT)
                .build();

        return ReplicationStatusVal.newBuilder()
                .setDataConsistent(isDataConsistent)
                .setSyncType(SyncType.LOG_ENTRY)
                .setRemainingEntriesToSend(writeLag)
                .setStatus(syncStatus)
                .setSnapshotSyncInfo(syncInfo)
                .build();
    }
}
