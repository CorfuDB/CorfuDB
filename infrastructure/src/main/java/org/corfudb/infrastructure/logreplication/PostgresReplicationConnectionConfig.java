package org.corfudb.infrastructure.logreplication;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.PgClusterManager.isTestEnvironment;

@Slf4j
public class PostgresReplicationConnectionConfig {
    public static boolean isPostgres = false;

    @Getter
    private String pgVersion;

    @Getter
    private String user;

    @Getter
    private String password;

    @Getter
    private String port;

    @Getter
    private String remotePgPort;

    @Getter
    private String dbName;

    @Getter
    private String tablesToReplicatePath;

    @Getter
    private String containerVirtualHost;

    @Getter
    private String containerPhysicalPort;

    public PostgresReplicationConnectionConfig(String filepath) {
        try (InputStream input = new FileInputStream(filepath)) {
            Properties prop = new Properties();
            prop.load(input);
            if (prop.get("pg_version") != null) {
                this.pgVersion = prop.getProperty("pg_version");
                this.user = prop.getProperty("pg_user");
                this.password = prop.getProperty("pg_password");
                this.port = prop.getProperty("pg_port");
                this.remotePgPort = prop.getProperty("remote_pg_port");
                this.dbName = prop.getProperty("pg_db_name");
                this.tablesToReplicatePath = prop.getProperty("tables_to_replicate_path");
                isPostgres = true;

                // For Testing: Used to alias host to docker pod host:port
                if (prop.getProperty("host_alias") != null) {
                    this.containerVirtualHost = prop.getProperty("host_alias");
                    this.containerPhysicalPort = prop.getProperty("container_port");

                    isTestEnvironment = true;
                    log.info("Starting as test environment!");
                }
            }
        } catch (IOException e) {
            log.info("Problem with loading config, starting as normal deployment!!!");
        }

        if (isPostgres) {
            log.info("Loaded postgres configuration!!!");
        }
    }
}
