package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public final class DefaultClusterConfig {

    @Getter
    private static final String defaultHost = "localhost";

    @Getter
    private static final List<String> activeNodesUuid =
            Arrays.asList("log-replication-0.log-replication.default.svc.cluster.local",
            "log-replication-1.log-replication.default.svc.cluster.local",
            "log-replication-2.log-replication.default.svc.cluster.local");

    @Getter
    private static final List<String> standbyNodesUuid =
            Arrays.asList("log-replication2-0.log-replication2.default.svc.cluster.local",
            "log-replication2-1.log-replication2.default.svc.cluster.local",
            "log-replication2-2.log-replication2.default.svc.cluster.local");

    @Getter
    private static final List<String> backupNodesUuid = Collections.singletonList("923e4567-e89b-12d3-a456-556642440000");

    @Getter
    private static final List<String> activeNodeNames =
            Arrays.asList("log-replication-0.log-replication.default.svc.cluster.local",
            "log-replication-1.log-replication.default.svc.cluster.local",
            "log-replication-2.log-replication.default.svc.cluster.local");

    @Getter
    private static final List<String> standbyNodeNames =
            Arrays.asList("log-replication2-0.log-replication.default.svc.cluster.local",
                    "log-replication2-1.log-replication.default.svc.cluster.local",
                    "log-replication2-2.log-replication.default.svc.cluster.local");

    @Getter
    private static final List<String> activeIpAddresses =
            Arrays.asList("log-replication-0.log-replication.default.svc.cluster.local",
            "log-replication-1.log-replication.default.svc.cluster.local",
            "log-replication-2.log-replication.default.svc.cluster.local");

    @Getter
    private static final List<String> standbyIpAddresses =
            Arrays.asList("log-replication2-0.log-replication2.default.svc.cluster.local",
            "log-replication2-1.log-replication2.default.svc.cluster.local",
            "log-replication2-2.log-replication2.default.svc.cluster.local");

    @Getter
    private static final String activeClusterId = "456e4567-e89b-12d3-a456-556642440001";

    @Getter
    private static final String activeCorfuPort = "9000";

    @Getter
    private static final String activeLogReplicationPort = "9010";

    @Getter
    private static final String standbyClusterId = "456e4567-e89b-12d3-a456-556642440002";

    @Getter
    private static final String standbyCorfuPort = "9000";

    @Getter
    private static final String standbyLogReplicationPort = "9010";

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

    public static String getDefaultNodeId() {
        String hostPath = "/etc/hosts";
        String nodeId = "";

        try {
            String content = new String(Files.readAllBytes(Paths.get(hostPath)), StandardCharsets.UTF_8);

            Pattern pattern = Pattern.compile("log-.*local");
            Matcher matcher = pattern.matcher(content);

            if(matcher.find()) {
                nodeId = matcher.group();
            }

        } catch (IOException e) {
            log.info("No nodeId Extracted!");
        }

        log.info("Extracted nodeId: {}", nodeId);
        return nodeId;
    }

    private DefaultClusterConfig() {

    }
}
