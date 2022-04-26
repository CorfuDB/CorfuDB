package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class DefaultClusterConfig {

    @Getter
    private static final String defaultHost = "localhost";

    @Getter
    private static final List<String> activeNodesUuid = Collections.singletonList("123e4567-e89b-12d3-a456-556642440000");

    @Getter
    private static final List<String> standbyNodesUuid = Arrays.asList("111e4567-e89b-12d3-a456-556642440111",
            "222e4567-e89b-12d3-a456-556642440222", "333e4567-e89b-12d3-a456-556642440333");

    @Getter
    private static final List<String> backupNodesUuid = Collections.singletonList("923e4567-e89b-12d3-a456-556642440000");

    @Getter
    private static final List<String> activeNodeNames = Collections.singletonList("active_site_node0");

    @Getter
    private static final List<String> standbyNodeNames = Arrays.asList("standby_site1_node0", "standby_site2_node0", "standby_site3_node0");

    @Getter
    private static final List<String> activeIpAddresses = Arrays.asList(defaultHost, defaultHost, defaultHost);

    @Getter
    private static final List<String> standbyIpAddresses = Collections.singletonList(defaultHost);

    @Getter
    private static final String activeClusterId = "456e4567-e89b-12d3-a456-556642440001";

    @Getter
    private static final String activeCorfuPort = "9000";

    @Getter
    private static final String activeLogReplicationPort = "9010";

    @Getter
    private static final List<String> standbyClusterIds = Arrays.asList("116e4567-e89b-12d3-a456-111664440011",
            "226e4567-e89b-12d3-a456-111664440022", "336e4567-e89b-12d3-a456-111664440033") ;

    @Getter
    private static final List<String> standbyCorfuPorts = Arrays.asList("9001", "9002", "9003");

    @Getter
    private static final String standbyLogReplicationPort1 ="9021";

    @Getter
    private static final String standbyLogReplicationPort2 ="9022";

    @Getter
    private static final String standbyLogReplicationPort3 = "9023";

    @Getter
    private static final String backupLogReplicationPort = "9030";

    @Getter
    private static final int logSenderBufferSize = 2;

    @Getter
    private static final int logSenderRetryCount = 5;

    @Getter
    private static final int logSenderResendTimer = 5000;

    @Getter
    private static final int logSenderTimeoutTimer = 5000;

    @Getter
    private static final boolean logSenderTimeout = true;

    @Getter
    private static final int logSinkBufferSize = 40;

    @Getter
    private static final int logSinkAckCycleCount = 4;

    @Getter
    private static final int logSinkAckCycleTimer = 1000;

    public static String getDefaultNodeId(String endpoint) {
        String port = endpoint.split(":")[1];
        switch (port) {
            case activeLogReplicationPort:
                return activeNodesUuid.get(0);
            case standbyLogReplicationPort1:
                return standbyNodesUuid.get(0);
            case standbyLogReplicationPort2:
                return standbyNodesUuid.get(1);
            case standbyLogReplicationPort3:
                return standbyNodesUuid.get(2);
            case backupLogReplicationPort:
                return backupNodesUuid.get(0);
            default:
                return null;
        }
    }

    private DefaultClusterConfig() {

    }
}
