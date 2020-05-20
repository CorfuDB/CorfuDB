package org.corfudb.infrastructure.logreplication;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.util.Properties;

/**
 * This class represents the Log Replication Transport Configuration.
 *
 * If a custom channel is used for Log Replication Server communication, this class collects
 * the relevant configuration of the adapter: JAR path name, implementation classes for both
 * server and client.
 */
@Data
@Slf4j
public class CorfuReplicationTransportConfig {

    public static final String DEFAULT_JAR_PATH = "/log-replication/target/log-replication-0.3.0-SNAPSHOT.jar";
    public static final String DEFAULT_SERVER_CLASSNAME = "org.corfudb.logreplication.infrastructure.GRPCLogReplicationServerChannel";
    public static final String DEFAULT_CLIENT_CLASSNAME = "org.corfudb.logreplication.runtime.GRPCLogReplicationClientChannelAdapter";

    private String adapterJARPath;
    private String adapterServerClassName;
    private String adapterClientClassName;

    public CorfuReplicationTransportConfig(String filepath) {
        try (InputStream input = new FileInputStream(filepath)) {
            Properties prop = new Properties();
            prop.load(input);
            this.adapterJARPath = prop.getProperty("adapter_JAR_path");
            this.adapterServerClassName = prop.getProperty("adapter_server_class_name");
            this.adapterClientClassName = prop.getProperty("adapter_client_class_name");
        } catch (IOException e) {
            log.warn("Exception caught while trying to load transport configuration from {}. Default configuration " +
                    "will be used.", filepath);
            // Default Configuration
            this.adapterJARPath = getParentDir() + DEFAULT_JAR_PATH;
            this.adapterClientClassName = DEFAULT_CLIENT_CLASSNAME;
            this.adapterServerClassName = DEFAULT_SERVER_CLASSNAME;
        }
    }

    private static String getParentDir() {
        try {
            File directory = new File("../infrastructure");
            return directory.getCanonicalFile().getParent();
        } catch (Exception e) {
            String message = "Failed to load default JAR for channel adapter";
            log.error(message, e);
            throw new UnrecoverableCorfuError(message);
        }
    }
}
