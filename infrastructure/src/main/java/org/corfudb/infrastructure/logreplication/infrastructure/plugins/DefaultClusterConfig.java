package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;

public final class DefaultClusterConfig {

    private DefaultClusterConfig() {

    }

    @Getter
    private static List<String> activeNodesUuid = Arrays.asList("123e4567-e89b-12d3-a456-556642440000");

    @Getter
    private static List<String> standbyNodesUuid = Arrays.asList("123e4567-e89b-12d3-a456-556642440123");

    @Getter
    private static List<String> activeNodeNames = Arrays.asList("standby_site_node0");

    @Getter
    private static List<String> activeIpAddresses = Arrays.asList("localhost", "localhost", "localhost");

    @Getter
    private static List<String> standbyIpAddresses = Arrays.asList("localhost");

    @Getter
    private static String activeClusterId = "456e4567-e89b-12d3-a456-556642440001";

    @Getter
    private static String activeCorfuPort = "9000";

    @Getter
    private static String activeLogReplicationPort = "9010";

    @Getter
    private static String standbyClusterId = "456e4567-e89b-12d3-a456-556642440002";

    @Getter
    private static String standbyCorfuPort = "9001";

    @Getter
    private static String standbyLogReplicationPort = "9020";

    @Getter
    private static int logSenderBufferSize = 20;

    @Getter
    private static int logSenderRetryCount = 5;

    @Getter
    private static int logSenderResendTimer = 5000;

    @Getter
    private static int logSenderTimeoutTimer = 5000;

    @Getter
    private static boolean logSenderTimeout = true;

    @Getter
    private static int logSinkBufferSize = 40;

    @Getter
    private static int logSinkAckCycleCount = 4;

    @Getter
    private static int logSinkAckCycleTimer = 1000;
}
