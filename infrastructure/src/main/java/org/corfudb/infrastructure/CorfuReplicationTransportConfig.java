package org.corfudb.infrastructure;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

    // TODO(Anny): make this automatically point to current directory root
    public static final String DEFAULT_JAR_PATH = "/Users/amartinezman/annym/workspace/CorfuDB/log-replication/target/log-replication-0.3.0-SNAPSHOT.jar";
    public static final String DEFAULT_SERVER_CLASSNAME = "org.corfudb.logreplication.infrastructure.GRPCLogReplicationServerChannel";
    public static final String DEFAULT_CLIENT_CLASSNAME = "org.corfudb.logreplication.runtime.GRPCLogReplicationClientChannelAdapter";

    private String adapterJARPath = DEFAULT_JAR_PATH;

    private String adapterServerClassName = DEFAULT_SERVER_CLASSNAME;

    private String adapterClientClassName = DEFAULT_CLIENT_CLASSNAME;

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
        }
    }
}
