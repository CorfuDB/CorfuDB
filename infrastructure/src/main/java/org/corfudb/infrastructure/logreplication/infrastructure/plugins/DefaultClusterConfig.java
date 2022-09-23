package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
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
    private final List<String> sourceNodeUuids =
        Arrays.asList("123e4567-e89b-12d3-a456-556642440000",
            "123e4567-e89b-12d3-a456-556642440001",
            "123e4567-e89b-12d3-a456-556642440002");

    @Getter
    private final List<String> sinkNodeUuids =
        Arrays.asList("123e4567-e89b-12d3-a456-556642440123",
            "123e4567-e89b-12d3-a456-556642440124",
            "123e4567-e89b-12d3-a456-556642440125");

    @Getter
    private final List<String> backupNodesUuid = Collections.singletonList("923e4567-e89b-12d3-a456-556642440000");

    @Getter
    private final List<String> sourceNodeNames = Collections.singletonList(
        "source_site_node0");

    @Getter
    private final List<String> sourceIpAddresses = Arrays.asList(defaultHost, defaultHost, defaultHost);

    @Getter
    private final List<String> sinkIpAddresses = Arrays.asList(defaultHost, defaultHost, defaultHost);

    @Getter
    private final List<String> sourceClusterIds =
        Arrays.asList("456e4567-e89b-12d3-a456-556642440001",
            "456e4567-e89b-12d3-a456-556642440003",
            "456e4567-e89b-12d3-a456-556642440005");

    @Getter
    private final List<String> sourceCorfuPorts = Arrays.asList("9000", "9002", "9004");

    @Getter
    private final List<String> sourceLogReplicationPorts =
        Arrays.asList("9010", "9011", "9012");

    @Getter
    private final List<String> sinkClusterIds = Arrays.asList(
        "456e4567-e89b-12d3-a456-556642440002",
        "456e4567-e89b-12d3-a456-556642440004",
        "456e4567-e89b-12d3-a456-556642440006");

    @Getter
    private final List<String> sinkCorfuPorts = Arrays.asList("9001",
        "9003", "9005");

    @Getter
    private final List<String> sinkLogReplicationPorts =
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

        int index = sourceLogReplicationPorts.indexOf(port);
        if (index != -1) {
            return sourceNodeUuids.get(index);
        } else {
            index = sinkLogReplicationPorts.indexOf(port);
            if (index != -1) {
                return sinkNodeUuids.get(index);
            }
        }
        return null;
    }
}
