package org.corfudb.infrastructure.logreplication;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class PostgresReplicationConnectionConfig {
    public static boolean isPostgres = false;

    @Getter
    private String PG_VERSION;

    @Getter
    private String USER;

    @Getter
    private String PASSWORD;

    @Getter
    private String PORT;

    @Getter
    private String REMOTE_PG_PORT;

    @Getter
    private String DB_NAME;

    @Getter
    private String TABLES_TO_REPLICATE_PATH;

    @Getter
    private String TABLES_TO_CREATE_PATH;

    public PostgresReplicationConnectionConfig(String filepath) {
        try (InputStream input = new FileInputStream(filepath)) {
            Properties prop = new Properties();
            prop.load(input);
            if (prop.get("pg_version") != null) {
                this.PG_VERSION = prop.getProperty("pg_version");
                this.USER = prop.getProperty("pg_user");
                this.PASSWORD = prop.getProperty("pg_password");
                this.PORT = prop.getProperty("pg_port");
                this.REMOTE_PG_PORT = prop.getProperty("remote_pg_port");
                this.DB_NAME = prop.getProperty("pg_db_name");
                this.TABLES_TO_REPLICATE_PATH = prop.getProperty("tables_to_replicate_path");
                this.TABLES_TO_CREATE_PATH = prop.getProperty("tables_to_create_path");
                isPostgres = true;
            }
        } catch (IOException e) {
            log.info("Problem with loading config, starting as normal deployment!!!");
        }

        if (isPostgres) {
            log.info("Loaded postgres configuration!!!");
        }
    }
}
