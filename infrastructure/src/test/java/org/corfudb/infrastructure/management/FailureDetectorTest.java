package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.runtime.clients.IClientRouter;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.NodeNames.A;
import static org.corfudb.infrastructure.NodeNames.B;
import static org.corfudb.infrastructure.NodeNames.C;

public class FailureDetectorTest {

//    @Test
//    public void testPollRound() {
//        final String localEndpoint = A;
//        NetworkStretcher ns = NetworkStretcher.builder()
//                .initialPollInterval(Duration.ofMillis(100))
//                .currentPeriod(Duration.ofMillis(100))
//                .maxPeriod(Duration.ofMillis(200))
//                .periodDelta(Duration.ofMillis(50))
//                .build();
//        ServerContext serverContext = Mockito.mock(ServerContext.class);
//        Mockito.when(serverContext.getLocalEndpoint()).thenReturn(localEndpoint);
//        FailureDetector failureDetector = new FailureDetector(serverContext);
//        failureDetector.setNetworkStretcher(ns);
//
//        long epoch = 1;
//        UUID clusterId = UUID.fromString("00000000-0000-0000-0000-000000000000");
//        ImmutableSet<String> allServers = ImmutableSet.of(A, B, C);
//        Map<String, IClientRouter> routerMap = new HashMap<>();
//        SequencerMetrics metrics = SequencerMetrics.READY;
//        ImmutableList<String> responsiveServers = ImmutableList.of(localEndpoint, B);
//
//        long start = System.currentTimeMillis();
//        FileSystemStats fsStats = Mockito.mock(FileSystemStats.class);
//        PollReport report = failureDetector.pollRound(
//                epoch, clusterId, allServers, routerMap, metrics, responsiveServers, fsStats
//        );
//        Duration time = Duration.ofMillis(System.currentTimeMillis() - start);
//        assertThat(time).isGreaterThan(Duration.ofMillis(450));
//        assertThat(time).isLessThan(Duration.ofSeconds(2));
//
//        assertThat(report.getReachableNodes()).isEmpty();
//        assertThat(report.getFailedNodes()).containsExactly(A, B, C);
//    }
}