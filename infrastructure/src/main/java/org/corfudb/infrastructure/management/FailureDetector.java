package org.corfudb.infrastructure.management;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.NodeView;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;

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
public class FailureDetector implements IDetector {


    /**
     * Number of iterations to execute to detect a failure in a round.
     */
    @Getter
    @Setter
    private int failureThreshold = 3;

    /**
     * Max duration for the responseTimeouts of the routers.
     * In the worst case scenario or in case of failed servers, their response timeouts will be
     * set to a maximum value of maxPeriodDuration.
     */
    @Getter
    @Setter
    private long maxPeriodDuration = 8_000L;

    /**
     * Minimum duration for the responseTimeouts of the routers.
     * Under ideal conditions the routers will have a response timeout set to this.
     */
    @Getter
    @Setter
    private long initPeriodDuration = 5_000L;

    /**
     * Response timeout for every router.
     */
    @Getter
    private long period = initPeriodDuration;

    /**
     * Interval between iterations in a pollRound.
     */
    @Getter
    @Setter
    private long interIterationInterval = 1_000;

    /**
     * Increments in which the period moves towards the maxPeriodDuration in every failed
     * iteration.
     */
    @Getter
    @Setter
    private long periodDelta = 1_000L;

    /**
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers generates pollReport.
     *
     * @param layout Current Layout
     */
    public PollReport poll(@Nonnull Layout layout,
                           @Nonnull CorfuRuntime corfuRuntime) {

        Map<String, IClientRouter> routerMap;

        // Collect and set all responsive servers in the members array.
        Set<String> allResponsiveServersSet = layout.getAllServers();
        allResponsiveServersSet.removeAll(layout.getUnresponsiveServers());
        List<String> members = new ArrayList<>(allResponsiveServersSet);

        log.debug("Responsive members to poll, {}", members);

        // Set up arrays for routers to the endpoints.
        routerMap = new HashMap<>();
        members.forEach(s -> {
            try {
                IClientRouter router = corfuRuntime.getRouter(s);
                router.setTimeoutResponse(period);
                routerMap.put(s, router);
            } catch (NetworkException ne) {
                log.error("Error creating router for {}", s);
            }
        });
        // Perform polling of all responsive servers.
        return pollRound(layout.getEpoch(), members, routerMap);

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
    private PollReport pollRound(long epoch,
                                 List<String> members,
                                 Map<String, IClientRouter> routerMap) {

        Map<String, Integer> responsesMap = new HashMap<>();
        // The out of phase epoch nodes are all those nodes which responded to the pings with a
        // wrong epoch exception. This is due to the pinged node being at a different epoch and
        // either this runtime needs to be updated or the node needs to catchup.
        Map<String, Long> expectedEpoch = new HashMap<>();
        boolean failuresDetected = false;

        // Node View map for analysis.
        Map<String, NodeView> nodeViewMap = new HashMap<>();

        // In each iteration we poll all the servers in the members list.
        for (int iteration = 0; iteration < failureThreshold; iteration++) {

            // Ping all nodes and await their responses.
            Map<String, CompletableFuture<NodeView>> pollCompletableFutures =
                    pollOnceAsync(members, routerMap, epoch);

            // Collect responses and increment response counters for successful pings.
            // Gather all nodeViews received in the responses in the nodeViewMap.
            Set<String> responses = collectResponsesAndVerifyEpochs(
                    members,
                    pollCompletableFutures,
                    expectedEpoch,
                    nodeViewMap);

            // Aggregate the responses.
            // Return false if we received responses from all the members - failure NOT present.
            failuresDetected = !responses.equals(new HashSet<>(members));
            if (!failuresDetected && expectedEpoch.isEmpty()) {
                break;
            } else {
                // Timeout is increased anytime a failure is encountered.
                // Failure includes both, unresponsive and outOfPhaseEpoch nodes.
                if (failuresDetected) {
                    period = getIncreasedPeriod();
                    tuneRoutersResponseTimeout(members, routerMap, new HashSet<>(members), period);
                }
                final int pollIteration = iteration;
                responses.forEach(s -> responsesMap.put(s, pollIteration));
            }
            Sleep.MILLISECONDS.sleepUninterruptibly(interIterationInterval);
        }

        // End round and generate report.

        Set<String> failed = new HashSet<>();

        // We can try to scale back the network latency time after every poll round.
        // If there are no failures, after a few rounds our polling period converges back to
        // initPeriodDuration.
        period = Math.max(initPeriodDuration, (period - periodDelta));
        tuneRoutersResponseTimeout(members, routerMap, new HashSet<>(members), period);

        // Check all responses and collect all failures.
        if (failuresDetected) {
            failed = members.stream()
                    .filter(s -> responsesMap.get(s) == null
                            || responsesMap.get(s) != (failureThreshold - 1))
                    .collect(Collectors.toSet());
        }
        // Reset the timeout of all the failed nodes to the max value to set a longer
        // timeout period to detect their response.
        tuneRoutersResponseTimeout(members, routerMap, failed, maxPeriodDuration);

        return new PollReport.PollReportBuilder()
                .pollEpoch(epoch)
                .failingNodes(failed)
                .outOfPhaseEpochNodes(expectedEpoch)
                .nodeViewMap(nodeViewMap)
                .build();
    }

    /**
     * Poll all members servers once asynchronously and store their futures in
     * pollCompletableFutures.
     *
     * @param members   All active members in the layout.
     * @param routerMap Map of routers for all active members.
     * @param epoch     Current epoch for the polling round to stamp the ping messages.
     * @return Map of Completable futures for the pings.
     */
    private Map<String, CompletableFuture<NodeView>> pollOnceAsync(List<String> members,
                                                                   Map<String, IClientRouter>
                                                                           routerMap,
                                                                   final long epoch) {
        // Poll servers for health.  All ping activity will happen in the background.
        Map<String, CompletableFuture<NodeView>> pollCompletableFutures = new HashMap<>();
        members.forEach(s -> {
            try {
                pollCompletableFutures.put(s, new ManagementClient(routerMap.get(s), epoch)
                        .sendHeartbeatRequest());
            } catch (Exception e) {
                CompletableFuture<NodeView> cf = new CompletableFuture<>();
                cf.completeExceptionally(e);
                pollCompletableFutures.put(s, cf);
            }
        });
        return pollCompletableFutures;
    }

    /**
     * Block on all poll futures and collect the responses. There are 3 possible cases.
     * 1. Receive a PONG. We set the responses[i] to the polling iteration number.
     * 2. WrongEpochException is thrown. We mark as a successful response but also record the
     * expected epoch.
     * 3. Other Exception is thrown. We do nothing. (The response[i] in this case is left behind.)
     */
    private Set<String> collectResponsesAndVerifyEpochs(
            List<String> members,
            Map<String, CompletableFuture<NodeView>> pollCompletableFutures,
            Map<String, Long> expectedEpoch,
            Map<String, NodeView> nodeViewMap) {
        // Collect responses and increment response counters for successful pings.
        Set<String> responses = new HashSet<>();
        members.forEach(s -> {
            try {
                NodeView nodeView = CFUtils
                        .within(pollCompletableFutures.get(s), Duration.ofMillis(period)).get();
                responses.add(s);
                expectedEpoch.remove(s);
                nodeViewMap.put(s, nodeView);
            } catch (Exception e) {
                if (e.getCause() instanceof WrongEpochException) {
                    responses.add(s);
                    expectedEpoch.put(s, ((WrongEpochException) e.getCause()).getCorrectEpoch());
                }
            }
        });
        return responses;
    }

    /**
     * Function to increment the existing response timeout period.
     *
     * @return The new calculated timeout value.
     */
    private long getIncreasedPeriod() {
        return Math.min(maxPeriodDuration, (period + periodDelta));
    }

    /**
     * Set the timeoutResponse for all the routers connected to the given endpoints with the
     * given value.
     *
     * @param endpoints Router endpoints.
     * @param timeout   New timeout value.
     */
    private void tuneRoutersResponseTimeout(List<String> members,
                                            Map<String, IClientRouter> routerMap,
                                            Set<String> endpoints,
                                            long timeout) {
        log.trace("Tuning router timeout responses for endpoints:{} to {}ms", endpoints, timeout);
        members.forEach(s -> {
            // Change timeout delay of routers of members list.
            if (endpoints.contains(s) && routerMap.get(s) != null) {
                routerMap.get(s).setTimeoutResponse(timeout);
            }
        });
    }
}
