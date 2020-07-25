package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole.ACTIVE;
import static org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole.STANDBY;

@Slf4j
public abstract class FileReaderClusterManager extends CorfuReplicationClusterManagerBaseAdapter {
    @Getter
    private class ClusterConfigKeys {
        @NonNull
        private final String rolePrefix;

        @NonNull
        private final LogReplicationClusterInfo.ClusterRole role;

        private String clusterRoleToRolePrefix(LogReplicationClusterInfo.ClusterRole role) {
            if (role == ACTIVE) {
                return "primary";
            } else if (role == STANDBY) {
                return "standby";
            } else {
                throw new IllegalArgumentException();
            }
        }

        public ClusterConfigKeys(LogReplicationClusterInfo.ClusterRole role) {
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

    abstract String getConfigFilePath();

    private ImmutableList<NodeDescriptor> getNodeDescriptors(ClusterConfigKeys keys, Properties props) {
        String primarySiteName = props.getProperty(keys.getSiteKey());
        int numPrimaryNodes = Integer.parseInt(props.getProperty(keys.getNumNodesKey()));
        return IntStream.range(0, numPrimaryNodes).boxed().map(i -> {
            String ipAddress = props.getProperty(keys.getIpAddressKey() + i);
            String port = props.getProperty(keys.getPortKey() + i);
            return new NodeDescriptor(ipAddress, port, primarySiteName, UUID.randomUUID());
        }).collect(ImmutableList.toImmutableList());
    }

    private ClusterDescriptor createClusterDescriptor(LogReplicationClusterInfo.ClusterRole role, Properties props) {
        ClusterConfigKeys keys = new ClusterConfigKeys(role);
        ImmutableList<NodeDescriptor> nodeDescriptors = getNodeDescriptors(keys, props);
        int corfuPort = Integer.parseInt(props.getProperty(keys.getCorfuPortKey()));
        return new ClusterDescriptor(UUID.randomUUID().toString(), role, corfuPort, nodeDescriptors);
    }

    @Override
    public LogReplicationClusterInfo.TopologyConfigurationMsg queryTopologyConfig(boolean useCashed) {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(getConfigFilePath())) {
            Properties props = new Properties();
            props.load(stream);
            ClusterDescriptor primarySiteDescriptor = createClusterDescriptor(ACTIVE, props);
            ClusterDescriptor standbySiteDescriptor = createClusterDescriptor(STANDBY, props);
            log.info("Primary: {}", primarySiteDescriptor);
            log.info("Standby: {}", standbySiteDescriptor);

            List<ClusterDescriptor> standbys = ImmutableList.of(standbySiteDescriptor);

            return new TopologyDescriptor(0, primarySiteDescriptor,
                    standbys).convertToMessage();

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
