package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class is an abstraction for all Log Replication plugin's configurations.
 *
 * Currently, four plugins are supported:
 * - Transport Plugin - defines the adapter to use for inter-cluster communication
 * - Site Information Plugin - defines the adapter to use for cluster information query
 * - Table Replication Specification Plugin - defines the adapter to use to pull the specified tables to be replicates
 * - Snapshot Sync Plugin - implements callbacks on snapshot sync boundaries (start/end) to implement any external logic
 *                          required for the duration of a snapshot sync.
 */
@Slf4j
@ToString
public class LogReplicationPluginConfig {

    // Transport Plugin
    public static final String DEFAULT_JAR_PATH = "/infrastructure/target/infrastructure-0.3.0-SNAPSHOT.jar";
    public static final String DEFAULT_SERVER_CLASSNAME = "org.corfudb.infrastructure.logreplication.transport.sample.NettyLogReplicationServerChannelAdapter";
    public static final String DEFAULT_CLIENT_CLASSNAME = "org.corfudb.infrastructure.logreplication.transport.sample.NettyLogReplicationClientChannelAdapter";

    // Stream Fetcher Plugin
    public static final String DEFAULT_STREAM_FETCHER_JAR_PATH = "/target/infrastructure-0.3.0-SNAPSHOT.jar";
    public static final String DEFAULT_STREAM_FETCHER_CLASSNAME = "org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultLogReplicationConfigAdapter";

    // Topology Manager Plugin
    public static final String DEFAULT_CLUSTER_MANAGER_CLASSNAME = "org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager";

    // Snapshot Sync Plugin
    public static final String DEFAULT_SNAPSHOT_SYNC_CLASSNAME = "org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultSnapshotSyncPlugin";

    @Getter
    private String transportAdapterJARPath;

    @Getter
    private String transportServerClassCanonicalName;

    @Getter
    private String transportClientClassCanonicalName;

    @Getter
    private String streamFetcherPluginJARPath;

    @Getter
    private String streamFetcherClassCanonicalName;

    @Getter
    private String topologyManagerAdapterJARPath;

    @Getter
    private String topologyManagerAdapterName;

    @Getter
    private String snapshotSyncPluginJARPath;

    @Getter
    private String snapshotSyncPluginCanonicalName;

    // nodeId file format:
    // node_id=99F30C42-F96D-A0A7-5531-468E52926A01
    @Getter
    private String nodeIdFilePath;

    public LogReplicationPluginConfig(String filepath) {
        try (InputStream input = new FileInputStream(filepath)) {
            Properties prop = new Properties();
            prop.load(input);
            this.transportAdapterJARPath = prop.getProperty("transport_adapter_JAR_path");
            this.transportServerClassCanonicalName = prop.getProperty("transport_adapter_server_class_name");
            this.transportClientClassCanonicalName = prop.getProperty("transport_adapter_client_class_name");

            this.streamFetcherPluginJARPath = prop.getProperty("stream_fetcher_plugin_JAR_path");
            this.streamFetcherClassCanonicalName = prop.getProperty("stream_fetcher_plugin_class_name");

            this.snapshotSyncPluginJARPath = prop.getProperty("snapshot_sync_plugin_JAR_path");
            this.snapshotSyncPluginCanonicalName = prop.getProperty("snapshot_sync_plugin_class_name");

            this.topologyManagerAdapterJARPath = prop.getProperty("topology_manager_adapter_JAR_path");
            this.topologyManagerAdapterName = prop.getProperty("topology_manager_adapter_class_name");

            this.nodeIdFilePath = prop.getProperty("local_node_id_path");
        } catch (IOException e) {
            log.warn("Plugin configuration not found {}. Default configuration will be used.", filepath);

            // Default Configurations
            this.transportAdapterJARPath = getParentDir() + DEFAULT_JAR_PATH;
            this.transportClientClassCanonicalName = DEFAULT_CLIENT_CLASSNAME;
            this.transportServerClassCanonicalName = DEFAULT_SERVER_CLASSNAME;

            this.streamFetcherPluginJARPath = getParentDir() + DEFAULT_STREAM_FETCHER_JAR_PATH;
            this.streamFetcherClassCanonicalName = DEFAULT_STREAM_FETCHER_CLASSNAME;

            this.snapshotSyncPluginJARPath = getParentDir() + DEFAULT_JAR_PATH;
            this.snapshotSyncPluginCanonicalName = DEFAULT_SNAPSHOT_SYNC_CLASSNAME;

            this.topologyManagerAdapterJARPath = getParentDir() + DEFAULT_JAR_PATH;
            this.topologyManagerAdapterName = DEFAULT_CLUSTER_MANAGER_CLASSNAME;

            this.nodeIdFilePath = null;
        }

        log.debug("{} ", this);
    }

    private static String getParentDir() {
        try {
            File directory = new File("../../../infrastructure");
            return directory.getCanonicalFile().getParent();
        } catch (Exception e) {
            String message = "Failed to load default JAR for channel adapter";
            log.error(message, e);
            throw new UnrecoverableCorfuError(message);
        }
    }
}
