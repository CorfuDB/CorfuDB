package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static org.corfudb.common.util.URLUtils.getPortFromEndpointURL;

public final class DefaultClusterConfig {

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
    private final List<String> activeNodeUuids =
        Arrays.asList("123e4567-e89b-12d3-a456-556642440000",
            "123e4567-e89b-12d3-a456-556642440001",
            "123e4567-e89b-12d3-a456-556642440002");

    @Getter
    private final List<String> standbyNodeUuids =
        Arrays.asList("123e4567-e89b-12d3-a456-556642440123",
            "123e4567-e89b-12d3-a456-556642440124",
            "123e4567-e89b-12d3-a456-556642440125");

    @Getter
    private final List<String> backupNodesUuid = Collections.singletonList("923e4567-e89b-12d3-a456-556642440000");

    @Getter
    private final List<String> activeNodeNames = Collections.singletonList(
        "active_site_node0");

    @Getter
    private final List<String> activeIpAddresses = Arrays.asList(defaultHost, defaultHost, defaultHost);

    @Getter
    private final List<String> standbyIpAddresses = Arrays.asList(defaultHost, defaultHost, defaultHost);

    @Getter
    private final List<String> activeClusterIds =
        Arrays.asList("456e4567-e89b-12d3-a456-556642440001",
            "456e4567-e89b-12d3-a456-556642440003",
            "456e4567-e89b-12d3-a456-556642440005");

    @Getter
    private final List<String> activeCorfuPorts = Arrays.asList("9000", "9002", "9004");

    @Getter
    private final List<String> activeLogReplicationPorts =
        Arrays.asList("9010", "9011", "9012");

    @Getter
    private final List<String> standbyClusterIds = Arrays.asList(
        "456e4567-e89b-12d3-a456-556642440002",
        "456e4567-e89b-12d3-a456-556642440004",
        "456e4567-e89b-12d3-a456-556642440006");

    @Getter
    private final List<String> standbyCorfuPorts = Arrays.asList("9001",
        "9003", "9005");

    @Getter
    private final List<String> standbyLogReplicationPorts =
        Arrays.asList("9020", "9021", "9022");

    @Getter
    private final String backupLogReplicationPort = "9030";

    @Getter
    private final int logSinkBufferSize = 40;

    @Getter
    private final int logSinkAckCycleCount = 4;

    @Getter
    private final int logSinkAckCycleTimer = 1000;

    public String getDefaultNodeId(String endpoint) {
        String port = getPortFromEndpointURL(endpoint);
        if (Objects.equals(port, backupLogReplicationPort)) {
            return backupNodesUuid.get(0);
        }

        int index = activeLogReplicationPorts.indexOf(port);
        if (index != -1) {
            return activeNodeUuids.get(index);
        } else {
            index = standbyLogReplicationPorts.indexOf(port);
            if (index != -1) {
                return standbyNodeUuids.get(index);
            }
        }
        return null;
    }
}
