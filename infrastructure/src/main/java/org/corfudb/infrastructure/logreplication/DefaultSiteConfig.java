package org.corfudb.infrastructure.logreplication;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;

public final class DefaultSiteConfig {

    private DefaultSiteConfig() {

    }

    @Getter
    private static List<String> primaryNodeNames = Arrays.asList("primary_site_node0", "primary_site_node1",
            "primary_site_node2");

    @Getter
    private static List<String> standbyNodeNames = Arrays.asList("standby_site_node0");

    @Getter
    private static List<String> primaryIpAddresses = Arrays.asList("localhost", "localhost", "localhost");

    @Getter
    private static List<String> standbyIpAddresses = Arrays.asList("localhost");

    @Getter
    private static String primarySiteName = "primary site";

    @Getter
    private static String primaryCorfuPort = "9000";

    @Getter
    private static String primaryLogReplicationPort = "9010";

    @Getter
    private static String standbySiteName = "standby site";

    @Getter
    private static String standbyCorfuPort = "9001";

    @Getter
    private static String standbyLogReplicationPort = "9020";

    @Getter
    private static int logWriterQueueSize = 20;

    @Getter
    private static int logWriterAckCycleCount = 1;

    @Getter
    private static int logWriterAckCycleTime = 100;
}
