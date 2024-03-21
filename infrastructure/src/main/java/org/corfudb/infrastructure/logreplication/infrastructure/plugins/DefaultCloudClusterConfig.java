package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.infrastructure.utils.CorfuSaasEndpointProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DefaultCloudClusterConfig extends DefaultClusterConfig {

    @Getter
    private static final int logSenderRetryCount = 5;

    @Getter
    private static final int logSenderBufferSize = 2;

    @Getter
    private static final int logSenderResendTimer = 5000;

    @Getter
    private static final int logSenderTimeoutTimer = 5000;

    @Getter
    private static final boolean logSenderTimeout = true;

    @Getter
    private final String defaultHost = "localhost";

    @Getter
    private final List<String> sourceNodeUuids =
            Arrays.asList("log-replication-0.log-replication.default.svc.cluster.local",
                    "log-replication-1.log-replication.default.svc.cluster.local",
                    "log-replication-2.log-replication.default.svc.cluster.local");

    @Getter
    private final List<String> sinkNodeUuids =
            Arrays.asList("log-replication2-0.log-replication2.default.svc.cluster.local",
                    "log-replication2-1.log-replication2.default.svc.cluster.local",
                    "log-replication2-2.log-replication2.default.svc.cluster.local");

    @Getter
    private final List<String> backupNodesUuid = Collections.singletonList("923e4567-e89b-12d3-a456-556642440000");

    @Getter
    private final List<String> sourceNodeNames =
            Arrays.asList("log-replication-0.log-replication.default.svc.cluster.local",
                "log-replication-1.log-replication.default.svc.cluster.local",
                "log-replication-2.log-replication.default.svc.cluster.local");

    @Getter
    private final List<String> sinkNodeNames =
            Arrays.asList("log-replication2-0.log-replication2.default.svc.cluster.local",
                    "log-replication2-1.log-replication2.default.svc.cluster.local",
                    "log-replication2-2.log-replication2.default.svc.cluster.local");

    @Getter
    private final List<String> sourceIpAddresses =
            Arrays.asList("log-replication-0.log-replication.default.svc.cluster.local",
                    "log-replication-1.log-replication.default.svc.cluster.local",
                    "log-replication-2.log-replication.default.svc.cluster.local");

    @Getter
    private final List<String> sinkIpAddresses =
            Arrays.asList("log-replication2-0.log-replication2.default.svc.cluster.local",
                    "log-replication2-1.log-replication2.default.svc.cluster.local",
                    "log-replication2-2.log-replication2.default.svc.cluster.local");

    @Getter
    private static final List<String> sourceClusterIds =
            Arrays.asList("456e4567-e89b-12d3-a456-556642440001");

    @Getter
    private static final List<String> backupClusterIds =
            Arrays.asList("456e4567-e89b-12d3-a456-556642440007");

    @Getter
    private final List<String> sourceCorfuPorts = Arrays.asList("9000", "9000", "9000");

    @Getter
    private final List<String> sourceLogReplicationPorts =
            Arrays.asList("9010", "9010", "9010");

    @Getter
    private static final List<String> sinkClusterIds =
            Arrays.asList("456e4567-e89b-12d3-a456-556642440002");

    @Getter
    private final List<String> sinkCorfuPorts = Arrays.asList("9000",
            "9000", "9000");

    @Getter
    private final List<String> sinkLogReplicationPorts =
            Arrays.asList("9010", "9010", "9010");

    @Getter
    private final String backupLogReplicationPort = "9030";

    @Getter
    private final int logSinkBufferSize = 40;

    @Getter
    private final int logSinkAckCycleCount = 4;

    @Getter
    private final int logSinkAckCycleTimer = 1000;

    public String getDefaultNodeId(String endpoint) {
        return CorfuSaasEndpointProvider.getLrCloudEndpoint();
    }
}
