package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.runtime.ExampleSchemas.ClusterUuidMsg;

@Slf4j
public class PgClusterManager extends DefaultClusterManager {

    @Getter
    private long configId;

    @Getter
    private boolean shutdown;

    @Getter
    public ClusterManagerCallback clusterManagerCallback;
    public static ConfigStreamService configStreamService;

    public void start() {
        configId = 0L;
        shutdown = false;
        topologyConfig = constructTopologyConfigMsg();
        clusterManagerCallback = new ClusterManagerCallback(this);
        configStreamService = new ConfigStreamService(this);

        Thread thread = new Thread(clusterManagerCallback);
        thread.start();
    }

    public static class ConfigStreamService {
        /* TODO: turn this into a constantly running task that will poll from config db table
         * and queue a task from changes it observes
         */
        private final DefaultClusterManager clusterManager;

        public ConfigStreamService(DefaultClusterManager clusterManager) {
            this.clusterManager = clusterManager;
        }

        public void queueTask(ClusterUuidMsg msg) {
            log.info("queueTask :: {}", msg);
            if (msg.equals(OP_SWITCH)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateConfigWithRoleSwitch());
            } else if (msg.equals(OP_TWO_ACTIVE)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateConfigWithAllActive());
            } else if (msg.equals(OP_ALL_STANDBY)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateConfigWithAllStandby());
            } else if (msg.equals(OP_INVALID)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateConfigWithInvalid());
            } else if (msg.equals(OP_RESUME)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateDefaultValidConfig());
            } else if (msg.equals(OP_ENFORCE_SNAPSHOT_FULL_SYNC)) {
                try {
                    clusterManager.forceSnapshotSync(clusterManager.queryTopologyConfig(true).getClustersList().get(1).getId());
                } catch (LogReplicationDiscoveryServiceException e) {
                    log.warn("Caught a RuntimeException ", e);
                    ClusterRole role = clusterManager.getCorfuReplicationDiscoveryService().getLocalClusterRoleType();
                    if (role != ClusterRole.STANDBY) {
                        log.error("The current cluster role is {} and should not throw a RuntimeException for forceSnapshotSync call.", role);
                        Thread.interrupted();
                    }
                }
            } else if (msg.equals(OP_BACKUP)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateConfigWithBackup());
            }
        }

    }
}
