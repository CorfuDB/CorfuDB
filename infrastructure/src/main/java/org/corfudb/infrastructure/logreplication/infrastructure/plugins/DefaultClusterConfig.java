package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class DefaultClusterConfig {

    private static final String activeLRPort1 = "9010";
    private static final String activeLRPort2 = "9011";
    private static final String standbyLRPort1 = "9020";
    private static final String standbyLRPort2 = "9021";

    @Getter
    private static final String defaultHost = "localhost";

    @Getter
    private static final List<String> activeNodesUuid = Arrays.asList(
        "123e4567-e89b-12d3-a456-556642440000", "123e4567-e89b-12d3-a456-556642440001");

    @Getter
    private static final List<String> standbyNodesUuid = Arrays.asList(
        "123e4567-e89b-12d3-a456-556642440123", "123e4567-e89b-12d3-a456-556642440124");

    @Getter
    private static final List<String> backupNodesUuid = Collections.singletonList("923e4567-e89b-12d3-a456-556642440000");

    @Getter
    private static final List<String> activeNodeNames = Arrays.asList(
        "active_site_node0", "active_site_node1");

    @Getter
    private static final List<String> standbyNodeNames = Arrays.asList(
        "standby_site_node0", "standby_site_node1");

    @Getter
    private static final List<String> activeIpAddresses = Arrays.asList(defaultHost, defaultHost, defaultHost);

    @Getter
    private static final List<String> standbyIpAddresses = Arrays.asList(defaultHost, defaultHost);

    @Getter
    private static final String activeClusterId = "456e4567-e89b-12d3-a456-556642440001";

    @Getter
    private static final String activeCorfuPort = "9000";

    @Getter
    private static final List<String> activeLogReplicationPort = Arrays.asList(
        activeLRPort1, activeLRPort2);

    @Getter
    private static final String standbyClusterId = "456e4567-e89b-12d3-a456-556642440002";

    @Getter
    private static final String standbyCorfuPort = "9001";

    @Getter
    private static final List<String> standbyLogReplicationPort = Arrays.asList(
        standbyLRPort1, standbyLRPort2);

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
            case activeLRPort1:
                return activeNodesUuid.get(0);
            case activeLRPort2:
                return activeNodesUuid.get(1);
            case standbyLRPort1:
                return standbyNodesUuid.get(0);
            case standbyLRPort2:
                return standbyNodesUuid.get(1);
            case backupLogReplicationPort:
                return backupNodesUuid.get(0);
            default:
                return null;
        }
    }

    private DefaultClusterConfig() {

    }
}
