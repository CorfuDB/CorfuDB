package org.corfudb.infrastructure.management;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;
import org.corfudb.util.Utils;

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
     * Members to poll in every round
     */
    private String[] members;

    /**
     * Response timeout for every router.
     */
    @Getter
    private long period;

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

    private int[] responses;
    private IClientRouter[] membersRouters;
    private CompletableFuture<Boolean>[] pollCompletableFutures = null;
    private long[] expectedEpoch;

    private final long INVALID_EPOCH = -1L;
    private final int INVALID_RESPONSE = -1;
    private long currentEpoch = INVALID_EPOCH;

    /**
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers generates pollReport.
     *
     * @param layout Current Layout
     */
    public PollReport poll(Layout layout, CorfuRuntime corfuRuntime) {

        // Performs setup and checks for changes in the layout to update failure count
        setup(layout, corfuRuntime);

        // Perform polling of all responsive servers.
        return pollRound();

    }

    /**
     * Triggered on every poll.
     * Sets up the responsive servers array. (All servers excluding unresponsive servers)
     * Resets the currentEpoch to the layout epoch.
     *
     * @param layout       Latest known layout
     * @param corfuRuntime Connected CorfuRuntime instance.
     */
    private void setup(Layout layout, CorfuRuntime corfuRuntime) {

        // Reset local copy of the epoch.
        currentEpoch = layout.getEpoch();

        // Collect and set all responsive servers in the members array.
        Set<String> allResponsiveServersSet = layout.getAllServers();
        allResponsiveServersSet.removeAll(layout.getUnresponsiveServers());
        members = allResponsiveServersSet.toArray(new String[allResponsiveServersSet.size()]);
        Arrays.sort(members);

        log.debug("Responsive members to poll, {}", new ArrayList<>(Arrays.asList(members)));

        // Set up arrays for routers to the endpoints.
        membersRouters = new IClientRouter[members.length];
        responses = new int[members.length];
        expectedEpoch = new long[members.length];
        period = initPeriodDuration;

        for (int i = 0; i < members.length; i++) {
            try {
                membersRouters[i] = corfuRuntime.getRouter(members[i]);
                membersRouters[i].setTimeoutResponse(period);
            } catch (NetworkException ne) {
                log.error("Error creating router for {}", members[i]);
            }
            expectedEpoch[i] = INVALID_EPOCH;
        }

        pollCompletableFutures = new CompletableFuture[members.length];
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
    private PollReport pollRound() {

        Set<String> membersSet = new HashSet<>(Arrays.asList(members));
        // At the start of the round reset all response counters.
        resetResponses();
        boolean failuresDetected = false;

        // In each iteration we poll all the servers in the members list.
        for (int iteration = 0; iteration < failureThreshold; iteration++) {

            // Ping all nodes and await their responses.
            pollOnceAsync();

            // Collect responses and increment response counters for successful pings.
            collectResponsesAndVerifyEpochs(iteration);

            failuresDetected = isFailurePresent(iteration);
            if (!failuresDetected) {
                break;
            } else {
                // Timeout is increased anytime a failure is encountered.
                // Failure includes both, unresponsive and outOfPhaseEpoch nodes.
                period = getIncreasedPeriod();
                tuneRoutersResponseTimeout(membersSet, period);
            }

            Sleep.MILLISECONDS.sleepUninterruptibly(interIterationInterval);
        }

        // End round and generate report.
        Map<String, Long> outOfPhaseEpochNodes = new HashMap<>();
        Set<String> failed = new HashSet<>();

        // We can try to scale back the network latency time after every poll round.
        // If there are no failures, after a few rounds our polling period converges back to
        // initPeriodDuration.
        period = Math.max(initPeriodDuration, (period - periodDelta));
        tuneRoutersResponseTimeout(membersSet, period);

        // Check all responses and collect all failures.
        if (failuresDetected) {
            for (int i = 0; i < members.length; i++) {
                if (responses[i] != (failureThreshold - 1)) {
                    failed.add(members[i]);
                }
                if (expectedEpoch[i] != INVALID_EPOCH) {
                    outOfPhaseEpochNodes.put(members[i], expectedEpoch[i]);
                    expectedEpoch[i] = INVALID_EPOCH;
                }
            }
        }
        // Reset the timeout of all the failed nodes to the max value to set a longer
        // timeout period to detect their response.
        tuneRoutersResponseTimeout(failed, maxPeriodDuration);

        return new PollReport.PollReportBuilder()
                .pollEpoch(currentEpoch)
                .failingNodes(failed)
                .outOfPhaseEpochNodes(outOfPhaseEpochNodes)
                .build();
    }

    /**
     * Reset all responses to an invalid iteration number, -1.
     */
    private void resetResponses() {
        for (int i = 0; i < responses.length; i++) {
            responses[i] = INVALID_RESPONSE;
        }
    }

    /**
     * Poll all members servers once asynchronously and store their futures in
     * pollCompletableFutures.
     */
    private void pollOnceAsync() {
        // Poll servers for health.  All ping activity will happen in the background.
        for (int i = 0; i < members.length; i++) {
            try {
                pollCompletableFutures[i] = membersRouters[i].getClient(BaseClient.class).ping();
            } catch (Exception e) {
                CompletableFuture<Boolean> cf = new CompletableFuture<>();
                cf.completeExceptionally(e);
                pollCompletableFutures[i] = cf;
            }
        }
    }

    /**
     * Block on all poll futures and collect the responses. There are 3 possible cases.
     * 1. Receive a PONG. We set the responses[i] to the polling iteration number.
     * 2. WrongEpochException is thrown. We mark as a successful response but also record the
     * expected epoch.
     * 3. Other Exception is thrown. We do nothing. (The response[i] in this case is left behind.)
     *
     * @param pollIteration poll iteration in the ongoing polling round.
     */
    private void collectResponsesAndVerifyEpochs(int pollIteration) {
        // Collect responses and increment response counters for successful pings.
        for (int i = 0; i < members.length; i++) {
            try {
                pollCompletableFutures[i].get();
                responses[i] = pollIteration;
                expectedEpoch[i] = INVALID_EPOCH;
            } catch (Exception e) {
                if (e.getCause() instanceof WrongEpochException) {
                    responses[i] = pollIteration;
                    expectedEpoch[i] = ((WrongEpochException) e.getCause()).getCorrectEpoch();
                }
            }
        }
    }

    /**
     * This method performs 2 tasks:
     * 1. Aggregates the successful responses in the responsiveNodes Set.
     * If the response was unsuccessful we increment the timeout period.
     * 2. Checks if there were no failures and no wrongEpochExceptions in which case the ongoing
     * polling round is completed. However, if failures were present (not epoch errors), then all
     * the routers' response timeouts are updated with the increased new period and the round
     * continues.
     *
     * @param pollIteration Iteration of the ongoing polling round.
     * @return True if failures were present. False otherwise.
     */
    private boolean isFailurePresent(int pollIteration) {

        // Aggregate the responses.
        Set<String> membersSet = new HashSet<>(Arrays.asList(members));
        Set<String> responsiveNodes = new HashSet<>();

        if (Arrays.stream(expectedEpoch).filter(l -> l != INVALID_EPOCH).count() == 0) {

            for (int i = 0; i < members.length; i++) {
                // If this counter is left behind and this node is not in members
                if (responses[i] == pollIteration) {
                    responsiveNodes.add(members[i]);
                }
            }

            // Return false if we received responses from all the members - failure NOT present.
            return !responsiveNodes.equals(membersSet);
        }

        // There are out of phase epochs present.
        return true;
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
    private void tuneRoutersResponseTimeout(Set<String> endpoints, long timeout) {
        log.trace("Tuning router timeout responses for endpoints:{} to {}ms", endpoints, timeout);
        for (int i = 0; i < members.length; i++) {
            // Change timeout delay of routers of members list.
            if (endpoints.contains(members[i]) && membersRouters[i] != null) {
                membersRouters[i].setTimeoutResponse(timeout);
            }
        }
    }
}
