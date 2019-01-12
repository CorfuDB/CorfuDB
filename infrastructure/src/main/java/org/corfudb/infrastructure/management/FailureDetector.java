package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.ClusterStateContext.HeartbeatCounter;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.ClusterState.ClusterStateBuilder;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.NodeState.HeartbeatTimestamp;
import org.corfudb.protocols.wireprotocol.NodeState.NodeConnectivityType;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * FailureDetector polls all the "responsive members" in the layout.
 * Responsive members: All endpoints that are responding to heartbeats. This list can be derived by
 * excluding unresponsiveServers from all endpoints.
 * For every poll method call invoked, it starts a polling round and generates a poll report.
 * Each polling round comprises of "failureThreshold" number of iterations.
 * - We asynchronously poll every known responsive member in the layout.
 * - Poll result aggregation.
 * - If we complete an iteration without detecting failures, we end the round successfully.
 * The management Server ensures only one instance of this class and hence this is NOT thread safe.
 * Created by zlokhandwala on 11/29/17.
 */
@Slf4j
@Builder
public class FailureDetector implements IDetector {

    @NonNull
    private final HeartbeatCounter heartbeatCounter;

    @NonNull
    private final String localEndpoint;

    /**
     * Number of iterations to execute to detect a failure in a round.
     */
    @Default
    private final int failureThreshold = 3;

    /**
     * Response timeout for every router.
     */
    @Default
    private final Duration period = Duration.ofSeconds(1);

    /**
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers generates pollReport.
     *
     * @param layout Current Layout
     */
    public PollReport poll(
            @Nonnull Layout layout, @Nonnull CorfuRuntime corfuRuntime, @NonNull  SequencerMetrics sequencerMetrics) {

        log.trace("Poll report. Layout: {}", layout);

        Map<String, IClientRouter> routerMap;

        // Collect and set all responsive servers in the members array.
        ImmutableSet<String> allServers = ImmutableSet.copyOf(layout.getAllServers());

        log.debug("Responsive members to poll, {}", allServers);

        // Set up arrays for routers to the endpoints.
        routerMap = new HashMap<>();
        allServers.forEach(s -> {
            try {
                IClientRouter router = corfuRuntime.getRouter(s);
                router.setTimeoutResponse(period.toMillis());
                routerMap.put(s, router);
            } catch (NetworkException ne) {
                log.error("Error creating router for {}", s);
            }
        });
        // Perform polling of all responsive servers.
        return pollRound(layout.getEpoch(), allServers, routerMap, sequencerMetrics);
    }

    /**
     * PollRound consists of iterations. In each iteration, the FailureDetector pings all the
     * responsive nodes in the layout. If a node fails to respond in an iteration, the failure
     * detector increments the ping timeout period and starts a new iteration. If one or more nodes
     * are still failing to respond after running a predefined max number of iterations,
     * the FailureDetector generates poll report, suggesting those nodes to be unresponsive.
     * If all nodes respond within an iteration, Failure Detector generates poll report with all
     * nodes responsive.
     *
     * @return Poll Report with detected failed nodes and out of phase epoch nodes.
     */
    private PollReport pollRound(
            long epoch, ImmutableSet<String> allServers, Map<String, IClientRouter> routerMap,
            SequencerMetrics sequencerMetrics) {

        boolean failuresDetected = false;

        // The out of phase epoch nodes are all those nodes which responded to the pings with a
        // wrong epoch exception. This is due to the pinged node being at a different epoch and
        // either this runtime needs to be updated or the node needs to catchup.
        Map<String, Long> wrongEpochs = new HashMap<>();
        Map<String, Integer> connectedNodesMap = new HashMap<>();
        PollReport latestReport = null;

        for (int iteration = 1; iteration <= failureThreshold; iteration++) {
            PollReport currReport = pollIteration(allServers, routerMap, epoch, sequencerMetrics);
            latestReport = currReport;

            wrongEpochs.putAll(currReport.getWrongEpochs());

            int pollIteration = iteration;
            currReport.getConnectedNodes().forEach(server -> {
                wrongEpochs.remove(server);
                connectedNodesMap.put(server, pollIteration);
            });

            // Aggregate the responses. Return false if we received responses from all the members - failure NOT present
            failuresDetected = !currReport.getFailedNodes().isEmpty();
            if (!failuresDetected && currReport.getWrongEpochs().isEmpty()) {
                break;
            }

            Sleep.MILLISECONDS.sleepUninterruptibly(period.toMillis());
        }

        if(latestReport == null){
            throw new IllegalStateException("Can't build poll report");
        }

        // Check all responses and collect all failures.
        ImmutableSet<String> failed = ImmutableSet.of();
        if (failuresDetected) {
            failed = allServers
                    .stream()
                    .filter(server -> !connectedNodesMap.containsKey(server))
                    .filter(server -> connectedNodesMap.get(server) != failureThreshold)
                    .collect(ImmutableSet.toImmutableSet());
        }

        return PollReport.builder()
                .pollEpoch(epoch)
                .connectedNodes(Sets.difference(allServers, failed).immutableCopy())
                .failedNodes(failed)
                .wrongEpochs(ImmutableMap.copyOf(wrongEpochs))
                .clusterState(latestReport.getClusterState())
                .build();
    }

    private PollReport pollIteration(
            ImmutableSet<String> allServers, Map<String, IClientRouter> router, long epoch,
            SequencerMetrics sequencerMetrics) {

        Map<String, CompletableFuture<NodeState>> asyncClusterState = pollAsync(allServers, router, epoch);

        Map<String, Long> wrongEpochs = new HashMap<>();
        Set<String> failedNodes = new HashSet<>();
        Set<String> connectedNodes = new HashSet<>();
        ClusterStateBuilder clusterStateBuilder = ClusterState.builder();

        asyncClusterState.forEach((server, state) -> {
            try {
                if(server.equals(localEndpoint)){
                    connectedNodes.add(localEndpoint);
                    return;
                }

                NodeState nodeState = state.get(period.toMillis(), TimeUnit.MILLISECONDS);
                clusterStateBuilder.node(server, nodeState);
                connectedNodes.add(server);
            } catch (Exception e) {
                if (e.getCause() instanceof WrongEpochException) {
                    wrongEpochs.put(server, ((WrongEpochException) e.getCause()).getCorrectEpoch());
                    return;
                }

                failedNodes.add(server);
                clusterStateBuilder.node(server, NodeState.getDefaultNodeState(server));
            }
        });

        //Get connectivity status for a local node
        Set<String> allConnectedNodes = Sets.union(connectedNodes, wrongEpochs.keySet());
        Map<String, Boolean> connectivity = allServers
                .stream()
                .collect(Collectors.toMap(Function.identity(), allConnectedNodes::contains));

        NodeState localNodeState = NodeState.builder()
                .endpoint(localEndpoint)
                .connectivityType(NodeConnectivityType.CONNECTED)
                .heartbeat(new HeartbeatTimestamp(epoch, heartbeatCounter.incrementHeartbeat()))
                .connectivity(connectivity)
                .sequencerMetrics(sequencerMetrics)
                .build();

        clusterStateBuilder.node(localEndpoint, localNodeState);

        return PollReport.builder()
                .pollEpoch(epoch)
                .connectedNodes(ImmutableSet.copyOf(connectedNodes))
                .failedNodes(ImmutableSet.copyOf(failedNodes))
                .wrongEpochs(ImmutableMap.copyOf(wrongEpochs))
                .clusterState(clusterStateBuilder.build())
                .build();
    }

    /**
     * Poll all members servers once asynchronously and store their futures in
     * pollCompletableFutures.
     *
     * @param allServers   All active members in the layout.
     * @param routerMap Map of routers for all active members.
     * @param epoch     Current epoch for the polling round to stamp the ping messages.
     * @return Map of Completable futures for the pings.
     */
    private Map<String, CompletableFuture<NodeState>> pollAsync(
            ImmutableSet<String> allServers, Map<String, IClientRouter> routerMap, long epoch) {
        // Poll servers for health.  All ping activity will happen in the background.
        Map<String, CompletableFuture<NodeState>> clusterState = new HashMap<>();
        allServers.forEach(s -> {
            try {
                clusterState.put(s, new ManagementClient(routerMap.get(s), epoch).sendNodeStateRequest());
            } catch (Exception e) {
                CompletableFuture<NodeState> cf = new CompletableFuture<>();
                cf.completeExceptionally(e);
                clusterState.put(s, cf);
            }
        });
        return clusterState;
    }
}
