package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
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

    public static final String DEFAULT_JAR_PATH = "/infrastructure/target/infrastructure-0.3.1-SNAPSHOT.jar";

    // Since the change is focused on GRPC.
    // TODO: Shama to remove uncomment/remove the default transport, once the change for netty is in
//    public static final String DEFAULT_SERVER_CLASSNAME = "org.corfudb.infrastructure.logreplication.transport.sample.NettyLogReplicationServerChannelAdapter";
//    public static final String DEFAULT_CLIENT_CLASSNAME = "org.corfudb.infrastructure.logreplication.transport.sample.NettyLogReplicationClientChannelAdapter";

    public static final String DEFAULT_SERVER_CLASSNAME = "org.corfudb.infrastructure.logreplication.transport.sample.GRPCLogReplicationServerChannelAdapter";
    public static final String DEFAULT_CLIENT_CLASSNAME = "org.corfudb.infrastructure.logreplication.transport.sample.GRPCLogReplicationClientChannelAdapter";

    // Stream Fetcher Plugin
    public static final String DEFAULT_STREAM_FETCHER_JAR_PATH = "/target/infrastructure-0.3.1-SNAPSHOT.jar";
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

    @Getter
    private String transportPluginSelectorJARPath;

    @Getter
    private String transportPluginSelectorClassCanonicalName;

    public LogReplicationPluginConfig(String filepath) {
        try (InputStream input = new FileInputStream(filepath)) {
            Properties prop = new Properties();
            prop.load(input);
            this.transportPluginSelectorJARPath = prop.getProperty("transport_plugin_selector_JAR_path");
            this.transportPluginSelectorClassCanonicalName = prop.getProperty("transport_plugin_selector_class_name");

            this.streamFetcherPluginJARPath = prop.getProperty("stream_fetcher_plugin_JAR_path");
            this.streamFetcherClassCanonicalName = prop.getProperty("stream_fetcher_plugin_class_name");

            this.snapshotSyncPluginJARPath = prop.getProperty("snapshot_sync_plugin_JAR_path");
            this.snapshotSyncPluginCanonicalName = prop.getProperty("snapshot_sync_plugin_class_name");

            this.topologyManagerAdapterJARPath = prop.getProperty("topology_manager_adapter_JAR_path");
            this.topologyManagerAdapterName = prop.getProperty("topology_manager_adapter_class_name");

            initTransportSelector(prop);

        } catch (IOException e) {
            log.warn("Plugin configuration not found {}. Default configuration will be used.", filepath);

            // Default Configurations
            this.transportPluginSelectorJARPath = getParentDir() + DEFAULT_JAR_PATH;
            this.transportPluginSelectorClassCanonicalName = DEFAULT_TRANSPORT_SELECTOR_CLASSNAME;

            this.streamFetcherPluginJARPath = getParentDir() + DEFAULT_STREAM_FETCHER_JAR_PATH;
            this.streamFetcherClassCanonicalName = DEFAULT_STREAM_FETCHER_CLASSNAME;

            this.snapshotSyncPluginJARPath = getParentDir() + DEFAULT_JAR_PATH;
            this.snapshotSyncPluginCanonicalName = DEFAULT_SNAPSHOT_SYNC_CLASSNAME;

            this.topologyManagerAdapterJARPath = getParentDir() + DEFAULT_JAR_PATH;
            this.topologyManagerAdapterName = DEFAULT_CLUSTER_MANAGER_CLASSNAME;

            initTransportSelector(null);
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

    private void initTransportSelector(Properties prop) {
        LogReplicationTransportSelectorPlugin transportSelector;
        File jar = new File(transportPluginSelectorJARPath);

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(transportPluginSelectorClassCanonicalName, true, child);
            transportSelector = (LogReplicationTransportSelectorPlugin) adapter.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
        }

        Pair<String, String> transportPropertyToRead = transportSelector.getTransportPropertyToReadFromConfig();
        if (prop != null) {
            this.transportAdapterJARPath = prop.getProperty("transport_adapter_JAR_path");
            this.transportClientClassCanonicalName = prop.getProperty(transportPropertyToRead.getKey());
            this.transportServerClassCanonicalName = prop.getProperty(transportPropertyToRead.getValue());
        } else {
            // plugin file not found. Defaults are being used.
            this.transportAdapterJARPath = getParentDir() + DEFAULT_JAR_PATH;
            if (transportPropertyToRead.getKey().contains("GRPC")) {
                this.transportClientClassCanonicalName = GRPC_DEFAULT_CLIENT_CLASSNAME;
                this.transportServerClassCanonicalName = GRPC_DEFAULT_SERVER_CLASSNAME;
            } else {
                this.transportClientClassCanonicalName = NETTY_DEFAULT_CLIENT_CLASSNAME;
                this.transportServerClassCanonicalName = NETTY_DEFAULT_SERVER_CLASSNAME;
            }
        }
    }
}
