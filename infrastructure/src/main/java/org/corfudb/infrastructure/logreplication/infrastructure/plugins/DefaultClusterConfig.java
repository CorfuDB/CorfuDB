package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.corfudb.common.util.URLUtils.getPortFromEndpointURL;

public final class DefaultClusterConfig {

    @Getter
    private static final String defaultHost = "localhost";

    @Getter
    private static final List<String> activeNodesUuid = Collections.singletonList("123e4567-e89b-12d3-a456-556642440000");

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
    private static final String activeClusterId = "456e4567-e89b-12d3-a456-556642440001";

    @Getter
    private static final String activeCorfuPort = "9000";

    @Getter
    private static final String activeLogReplicationPort = "9010";

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
    private static final int logSenderWaitPeriod = 30000;

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
        String port = getPortFromEndpointURL(endpoint);
        switch (port) {
            case activeLogReplicationPort:
                return activeNodesUuid.get(0);
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
