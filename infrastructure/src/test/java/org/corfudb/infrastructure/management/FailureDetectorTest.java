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
import java.util.Optional;

public class FailureDetectorTest {

    @Test
    public void testPollRound() {
        final String localEndpoint = A;
        FailureDetector.PollConfig config = FailureDetector.PollConfig
                .builder()
                .maxPollRounds(10)
                .maxDetectionDuration(Duration.ofSeconds(1))
                .maxSleepBetweenRetries(Duration.ofMillis(200))
                .initSleepBetweenRetries(Duration.ofMillis(100))
                .jitterFactor(0)
                .build();
        ServerContext serverContext = Mockito.mock(ServerContext.class);
        Mockito.when(serverContext.getLocalEndpoint()).thenReturn(localEndpoint);
        FailureDetector failureDetector = new FailureDetector(serverContext);
        failureDetector.setPollConfig(Optional.of(config));
        long epoch = 1;
        UUID clusterId = UUID.fromString("00000000-0000-0000-0000-000000000000");
        ImmutableSet<String> allServers = ImmutableSet.of(A, B, C);
        Map<String, IClientRouter> routerMap = new HashMap<>();
        SequencerMetrics metrics = SequencerMetrics.READY;
        ImmutableList<String> responsiveServers = ImmutableList.of(localEndpoint, B);
        FileSystemStats fsStats = Mockito.mock(FileSystemStats.class);
        PollReport report = failureDetector.pollRoundExpBackoff(
                epoch, clusterId, allServers, routerMap, metrics, responsiveServers, fsStats);

        Duration time = report.getElapsedTime();
        assertThat(time).isLessThanOrEqualTo(Duration.ofMillis(1000));

        assertThat(report.getReachableNodes()).isEmpty();
        assertThat(report.getFailedNodes()).containsExactly(A, B, C);
    }
}