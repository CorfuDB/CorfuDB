package org.corfudb.infrastructure.logreplication.PgUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresUtils {

    private PostgresUtils() {}

    public static boolean tryExecuteCommand(String sql, PostgresConnector connector) {
        boolean successOrExists = false;

        if (!sql.isEmpty()) {
            try (Connection conn = DriverManager.getConnection(connector.URL, connector.USER, connector.PASSWORD)) {

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
                    log.info("ERROR", e);
                }
            }
        }
        return successOrExists;
    }



    public static List<Map<String, Object>> executeQuery(String sql, PostgresConnector connector) {
        List<Map<String, Object>> result = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(connector.URL, connector.USER, connector.PASSWORD)) {
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
           log.info("ERROR", e);
        }
        return result;
    }

    public static List<String> createTablesCmds (List<JsonNode> tablesToCreate) {
        List<String> createTableCmds = new ArrayList<>();

        for (JsonNode table : tablesToCreate) {
            StringBuilder createCmd = new StringBuilder();

            createCmd.append(String.format("CREATE TABLE %s (",  table.get("table_name").asText()));

            for (JsonNode column : table.get("columns")) {
                createCmd.append(String.format("%s %s", column.get("name").asText(), column.get("data_type").asText()));

                if (column.has("primary_key")) {
                    createCmd.append(" PRIMARY KEY");
                }

                createCmd.append(", ");
            }

            createCmd.replace(createCmd.length() - 2, createCmd.length(), "");
            createCmd.append(");");
            createTableCmds.add(createCmd.toString());
        }

        return createTableCmds;
    }

    public static String createPublicationCmd(Set<String> tablesToReplicate, PostgresConnector connector) {
        return "CREATE PUBLICATION \"" +
                String.join("_", connector.ADDRESS.split("\\.")) +
                "_pub\"" +
                " FOR TABLE " +
                String.join(", ", tablesToReplicate) +
                ";";
    }

    public static String createSubscriptionCmd(PostgresConnector primary, PostgresConnector replica) {
        int max_retry = 10;
        String createSubCmd = "";
        String primaryPrefix = String.join("_", primary.ADDRESS.split("\\."));
        String replicaPrefix = String.join("_", replica.ADDRESS.split("\\."));
        String pubName = String.join("_", primaryPrefix, "pub");

        for (int i = 0; i < max_retry; i++) {
            String pubExistsQuery = String.format("SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '%s');", pubName);
            log.info("check exists: {}", pubExistsQuery);
            try {
                List<Map<String, Object>> queryResult = executeQuery(pubExistsQuery, primary);

                if (!queryResult.isEmpty()) {
                    boolean publicationExists = (boolean) queryResult.get(0).values().stream().findAny().get();
                    if (publicationExists) {
                        String subName = String.join("_", replicaPrefix, "sub");
                        createSubCmd = String.format("CREATE SUBSCRIPTION \"%s\" CONNECTION 'host=%s port=%s user=%s dbname=%s password=%s' PUBLICATION \"%s\";",
                                subName, primary.ADDRESS, primary.PORT, primary.USER, primary.DATABASE_NAME, primary.PASSWORD, pubName);
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

        log.info("CREATE_SUB_CMD: {}", createSubCmd);

        return createSubCmd;
    }

    public static List<String> getAllSubscriptions(PostgresConnector connector) {
        List<String> subscriptionNames = new ArrayList<>();
        String getSubQuery = "SELECT * FROM pg_subscription;";

        for (Map<String, Object> row : executeQuery(getSubQuery, connector)) {
            String subname = row.get("subname").toString();
            if (subname != null && !subname.isEmpty()) {
                subscriptionNames.add(subname);
            }
        }

        return subscriptionNames;
    }

    public static void dropSubscriptions(List<String> subscriptionsToDrop, PostgresConnector connector) {
        // TODO (POSTGRES): Add retry logic for failed drops, can also decouple from slot to guarantee
        //  drop and have service to clean up inactive slots

        String dropPrefix = "DROP SUBSCRIPTION ";
        for (String subscription : subscriptionsToDrop) {
            if (!tryExecuteCommand(dropPrefix + subscription + ";", connector)) {
                log.info("Unable to drop subscription: {}", subscription);
            } else {
                log.info("Dropped subscription: {}", subscription);
            }
        }
    }

    public static void truncateTables(List<String> tablesToTruncate, PostgresConnector connector) {
        String truncatePrefix = "TRUNCATE TABLE ";
        for (String table : tablesToTruncate) {
            if (!tryExecuteCommand(truncatePrefix + table + ";", connector)) {
                log.info("Unable to truncate table: {}", table);
            } else {
                log.info("Truncated table: {}", table);
            }
        }
    }

    public static List<String> getAllPublications(PostgresConnector connector) {
        List<String> publicationNames = new ArrayList<>();
        String getSubQuery = "SELECT * FROM pg_publication;";

        for (Map<String, Object> row : executeQuery(getSubQuery, connector)) {
            String pubname = row.get("pubname").toString();
            if (pubname != null && !pubname.isEmpty()) {
                publicationNames.add(pubname);
            }
        }

        return publicationNames;
    }

    public static void dropPublications(List<String> publicationsToDrop, PostgresConnector connector) {
        String dropPrefix = "DROP PUBLICATION ";
        for (String subscription : publicationsToDrop) {
            if (!tryExecuteCommand(dropPrefix + subscription + ";", connector)) {
                log.info("Unable to drop publication: {}", subscription);
            } else {
                log.info("Dropped publication: {}", subscription);
            }
        }
    }

    public static void dropAllSubscriptions(PostgresConnector connector) {
        dropSubscriptions(getAllSubscriptions(connector), connector);
    }

    public static void dropAllPublications(PostgresConnector connector) {
        dropPublications(getAllPublications(connector), connector);
    }
}
