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
 * This class is an abstraction for all Log Replication plugin's configurations.
 *
 * Currently, three plugins are supported:
 * - Transport Plugin - defines the adapter to use for inter-site communication
 * - Site Information Plugin - defines the adapter to use for site information query
 * - Table Replication Specification Plugin - defines the adapter to use to pull the specified tables to be replicates
 */
@Data
@Slf4j
public class LogReplicationPluginConfig {

    // Transport Configurations
    public static final String DEFAULT_JAR_PATH = "/log-replication/target/log-replication-0.3.0-SNAPSHOT.jar";
    public static final String DEFAULT_SERVER_CLASSNAME = "org.corfudb.logreplication.infrastructure.GRPCLogReplicationServerChannel";
    public static final String DEFAULT_CLIENT_CLASSNAME = "org.corfudb.logreplication.runtime.GRPCLogReplicationClientChannelAdapter";
    public static final String DEFAULT_STREAM_FETCHER_JAR_PATH = "/target/log-replication-0.3.0-SNAPSHOT.jar";
    public static final String DEFAULT_STREAM_FETCHER_CLASSNAME = "org.corfudb.logreplication.runtime.DefaultStreamFetcherPlugin";

    private String transportAdapterJARPath;
    private String transportServerClassCanonicalName;
    private String transportClientClassCanonicalName;
    private String streamFetcherPluginJARPath;
    private String streamFetcherClassCanonicalName;

    public LogReplicationPluginConfig(String filepath) {
        try (InputStream input = new FileInputStream(filepath)) {
            Properties prop = new Properties();
            prop.load(input);
            this.transportAdapterJARPath = prop.getProperty("transport_adapter_JAR_path");
            this.transportServerClassCanonicalName = prop.getProperty("transport_adapter_server_class_name");
            this.transportClientClassCanonicalName = prop.getProperty("transport_adapter_client_class_name");
            this.streamFetcherPluginJARPath = prop.getProperty("stream_fetcher_plugin_JAR_path");
            this.streamFetcherClassCanonicalName = prop.getProperty("stream_fetcher_plugin_class_name");
        } catch (IOException e) {
            log.warn("Exception caught while trying to load transport configuration from {}. Default configuration " +
                    "will be used.", filepath);
            // Default Configuration
            this.transportAdapterJARPath = getTransportParentDir() + DEFAULT_JAR_PATH;
            this.transportClientClassCanonicalName = DEFAULT_CLIENT_CLASSNAME;
            this.transportServerClassCanonicalName = DEFAULT_SERVER_CLASSNAME;
            this.streamFetcherPluginJARPath = getStreamFetcherParentDir() + DEFAULT_STREAM_FETCHER_JAR_PATH;
            this.streamFetcherClassCanonicalName = DEFAULT_STREAM_FETCHER_CLASSNAME;
        }
    }

    private static String getTransportParentDir() {
        try {
            File directory = new File("../infrastructure");
            return directory.getCanonicalFile().getParent();
        } catch (Exception e) {
            String message = "Failed to load default JAR for channel adapter";
            log.error(message, e);
            throw new UnrecoverableCorfuError(message);
        }
    }

    private static String getStreamFetcherParentDir() {
        try {
            File directory = new File("../log-replication");
            return directory.getCanonicalFile().getParent();
        } catch (Exception e) {
            String message = "Failed to load default JAR for stream fetcher plugin";
            log.error(message, e);
            throw new UnrecoverableCorfuError(message);
        }
    }
}
