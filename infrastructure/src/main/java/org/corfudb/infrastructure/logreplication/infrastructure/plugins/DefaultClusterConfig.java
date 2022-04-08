package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class DefaultClusterConfig {

    @Getter
    private static final String defaultHost = "localhost";

    @Getter
    private static final List<String> activeNodesUuid1 =
        Collections.singletonList("123e4567-e89b-12d3-a456-556642440000");

    @Getter
    private static final List<String> activeNodesUuid2 =
        Collections.singletonList("123e4567-e89b-12d3-a456-556642440001");

    @Getter
    private static final List<String> activeNodesUuid3 =
        Collections.singletonList("123e4567-e89b-12d3-a456-556642440002");

    @Getter
    private static final List<String> standbyNodesUuid = Collections.singletonList("123e4567-e89b-12d3-a456-556642440123");

    @Getter
    private static final List<String> backupNodesUuid = Collections.singletonList("923e4567-e89b-12d3-a456-556642440000");

    @Getter
    private static final List<String> activeNodeNames = Collections.singletonList("standby_site_node0");

    @Getter
    private static final List<String> activeIpAddresses = Arrays.asList(defaultHost, defaultHost, defaultHost);

    @Getter
    private static final List<String> standbyIpAddresses = Collections.singletonList(defaultHost);

    @Getter
    private static final String activeClusterId1 = "456e4567-e89b-12d3-a456-556642440001";

    @Getter
    private static final String activeClusterId2 = "456e4567-e89b-12d3-a456-556642440003";

    @Getter
    private static final String activeClusterId3 = "456e4567-e89b-12d3-a456-556642440005";

    @Getter
    private static final String activeCorfuPort1 = "9000";
    @Getter
    private static final String activeCorfuPort2 = "9002";
    @Getter
    private static final String activeCorfuPort3 = "9004";


    @Getter
    private static final String activeLogReplicationPort1 = "9010";
    @Getter
    private static final String activeLogReplicationPort2 = "9011";
    @Getter
    private static final String activeLogReplicationPort3 = "9012";

    @Getter
    private static final String standbyClusterId = "456e4567-e89b-12d3-a456-556642440002";

    @Getter
    private static final String standbyCorfuPort = "9001";

    @Getter
    private static final String standbyLogReplicationPort = "9020";

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
            case activeLogReplicationPort1:
                return activeNodesUuid1.get(0);
            case activeLogReplicationPort2:
                return activeNodesUuid2.get(0);
            case activeLogReplicationPort3:
                return activeNodesUuid3.get(0);
            case standbyLogReplicationPort:
                return standbyNodesUuid.get(0);
            case backupLogReplicationPort:
                return backupNodesUuid.get(0);
            default:
                return null;
        }
    }

    private DefaultClusterConfig() {

    }
}
