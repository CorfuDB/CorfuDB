package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.runtime.clients.IClientRouter;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class FailureDetectorTest {

    @Test
    public void testPollRound() {
        final String endpoint = "a";
        NetworkStretcher ns = NetworkStretcher.builder()
                .initialPollInterval(Duration.ofMillis(100))
                .currentPeriod(Duration.ofMillis(100))
                .maxPeriod(Duration.ofMillis(200))
                .periodDelta(Duration.ofMillis(50))
                .build();
        FailureDetector failureDetector = new FailureDetector(endpoint);
        failureDetector.setNetworkStretcher(ns);

        long epoch = 1;
        UUID clusterId = UUID.fromString("00000000-0000-0000-0000-000000000000");
        ImmutableSet<String> allServers = ImmutableSet.of("a", "b", "c");
        Map<String, IClientRouter> routerMap = new HashMap<>();
        SequencerMetrics metrics = SequencerMetrics.READY;
        ImmutableList<String> responsiveServers = ImmutableList.of("a", "b");

        long start = System.currentTimeMillis();
        PollReport report = failureDetector.pollRound(
                epoch, clusterId, allServers, routerMap, metrics, responsiveServers
        );
        Duration time = Duration.ofMillis(System.currentTimeMillis() - start);
        assertThat(time).isGreaterThan(Duration.ofMillis(450));
        assertThat(time).isLessThan(Duration.ofSeconds(2));

        assertThat(report.getReachableNodes()).isEmpty();
        assertThat(report.getFailedNodes()).containsExactly("a", "b", "c");
    }
}