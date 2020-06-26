package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole.ACTIVE;
import static org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole.STANDBY;

@Slf4j
public class UnitTestClusterManager extends CorfuReplicationClusterManagerAdapter {

    @Getter
    private class SiteConfigKeys {
        @NonNull
        private final String rolePrefix;

        @NonNull
        private final ClusterRole role;

        private String clusterRoleToRolePrefix(ClusterRole role) {
            if (role == ACTIVE) {
                return "primary";
            } else if (role == STANDBY) {
                return "standby";
            } else {
                throw new IllegalArgumentException();
            }
        }

        public SiteConfigKeys(ClusterRole role) {
            this.rolePrefix = clusterRoleToRolePrefix(role);
            this.role = role;
        }

        private String getSiteKey() {
            return rolePrefix + "_site";
        }

        private String getCorfuPortKey() {
            return rolePrefix + "_site_corfu_portnumber";
        }

        private String getNumNodesKey() {
            return rolePrefix + "_nodes";
        }

        private String getIpAddressKey() {
            return rolePrefix + "_site_node";
        }

        private String getPortKey() {
            return rolePrefix + "_site_portnumber";
        }
    }

    private static final String STANDBY_SITE_KEY = "standby_site";

    private final String configPath;

    public UnitTestClusterManager(String configPath) {
        this.configPath = configPath;
    }

    private ImmutableList<NodeDescriptor> getNodeDescriptors(SiteConfigKeys keys, Properties props) {
        String primarySiteName = props.getProperty(keys.getSiteKey());
        int numPrimaryNodes = Integer.parseInt(props.getProperty(keys.getNumNodesKey()));
        return IntStream.range(0, numPrimaryNodes).boxed().map(i -> {
            String ipAddress = props.getProperty(keys.getIpAddressKey() + i);
            String port = props.getProperty(keys.getPortKey() + i);
            return new NodeDescriptor(ipAddress, port, keys.getRole(),
                    primarySiteName, UUID.randomUUID());
        }).collect(ImmutableList.toImmutableList());
    }

    private ClusterDescriptor createClusterDescriptor(ClusterRole role, Properties props) {
        SiteConfigKeys keys = new SiteConfigKeys(role);
        ImmutableList<NodeDescriptor> nodeDescriptors = getNodeDescriptors(keys, props);
        int corfuPort = Integer.parseInt(props.getProperty(keys.getCorfuPortKey()));
        return new ClusterDescriptor(UUID.randomUUID().toString(), role, corfuPort, nodeDescriptors);
    }

    @Override
    public TopologyConfigurationMsg queryTopologyConfig() {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(configPath)) {
            Properties props = new Properties();
            props.load(stream);
            ClusterDescriptor primarySiteDescriptor = createClusterDescriptor(ACTIVE, props);
            ClusterDescriptor standbySiteDescriptor = createClusterDescriptor(STANDBY, props);
            log.info("Primary: {}", primarySiteDescriptor);
            log.info("Standby: {}", standbySiteDescriptor);
            ImmutableMap<String, ClusterDescriptor> standbySiteDescriptors =
                    ImmutableMap.of(props.getProperty(STANDBY_SITE_KEY), standbySiteDescriptor);
            return new TopologyDescriptor(0, primarySiteDescriptor,
                    standbySiteDescriptors).convertToMessage();

        } catch (IOException fnfe) {
            log.error("File not found: ", fnfe);
            throw new RuntimeException(fnfe);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }
}
