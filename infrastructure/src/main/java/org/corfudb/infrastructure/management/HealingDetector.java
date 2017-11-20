package org.corfudb.infrastructure.management;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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
 * HealingDetector polls all the "unresponsive members" in the layout.
 * Unresponsive members: All endpoints in the unresponsiveServers list in the layout.
 * For every poll method call invoked, it starts a polling round and generates a poll report.
 * Each polling round comprises of "detectionThreshold" number of iterations.
 * - We asynchronously poll every known unresponsive member in the layout.
 * - Poll result aggregation.
 * - If we complete an iteration without detecting any healed nodes, we end the round.
 * - Else we continue polling and generate the report with the healed node.
 * The management Server ensures only one instance of this class and hence this is NOT thread safe.
 * Created by zlokhandwala on 11/29/17.
 */
@Slf4j
public class HealingDetector implements IDetector {

    /**
     * Members to poll in every round
     */
    private String[] members;

    /**
     * Number of iterations to execute to detect a healed node in a round.
     */
    @Getter
    @Setter
    private int detectionThreshold = 3;

    /**
     * Threshold timeout to detect healing nodes.
     * This should be kept lower if there is no tolerance for high latency recovering nodes.
     */
    @Getter
    @Setter
    private long detectionPeriodDuration = 5_000L;

    /**
     * Interval between iterations in a pollRound.
     */
    @Getter
    @Setter
    private long interIterationInterval = 1_000;

    private int[] responses;
    private IClientRouter[] membersRouters;
    private CompletableFuture<Boolean>[] pollCompletableFutures = null;

    private final long INVALID_EPOCH = -1L;
    private final int INVALID_RESPONSE = -1;
    private long currentEpoch = INVALID_EPOCH;

    /**
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers and generates the report.
     *
     * @param layout Current Layout
     */
    public PollReport poll(Layout layout, CorfuRuntime corfuRuntime) {

        // Performs setup and checks for changes in the layout to update unresponsive servers list.
        setup(layout, corfuRuntime);

        // Perform polling of all unresponsive servers.
        return pollRound();

    }

    /**
     * Triggered on every poll.
     * Sets up the unresponsive servers array. (All servers in the unresponsiveServers list in
     * the layout)
     * Resets the currentEpoch to the layout epoch.
     *
     * @param layout       Latest known layout
     * @param corfuRuntime Connected CorfuRuntime instance.
     */
    private void setup(Layout layout, CorfuRuntime corfuRuntime) {

        currentEpoch = layout.getEpoch();

        // Get all unresponsive servers.
        members = layout.getUnresponsiveServers().stream().toArray(String[]::new);
        Arrays.sort(members);

        log.debug("Unresponsive members to poll, {}", new ArrayList<>(Arrays.asList(members)));
        membersRouters = new IClientRouter[members.length];
        responses = new int[members.length];

        for (int i = 0; i < members.length; i++) {
            try {
                membersRouters[i] = corfuRuntime.getRouter(members[i]);
                membersRouters[i].setTimeoutResponse(detectionPeriodDuration);
            } catch (NetworkException ne) {
                log.debug("Error creating router for {}", members[i]);
            }
        }
        pollCompletableFutures = new CompletableFuture[members.length];
    }

    /**
     * Each PollRound executes "detectionThreshold" number of iterations and generates the
     * poll report.
     *
     * @return Poll Report with detected healed nodes.
     */
    private PollReport pollRound() {

        Set<String> membersSet = new HashSet<>(Arrays.asList(members));
        Set<String> healed = new HashSet<>();
        resetResponses();

        // In each iteration we poll all the servers in the members list.
        for (int iteration = 0; iteration < detectionThreshold; iteration++) {

            // Ping all nodes and await their responses.
            pollOnceAsync();

            // Collect all poll responses.
            collectResponsesAndVerifyEpochs(iteration);

            if (!isHealedNodesPresent(membersSet)) {
                break;
            }

            Sleep.MILLISECONDS.sleepUninterruptibly(interIterationInterval);
        }

        // Check all responses and  report all healed nodes.
        for (int i = 0; i < members.length; i++) {
            if (responses[i] == (detectionThreshold - 1)) {
                healed.add(members[i]);
            }
        }

        return new PollReport.PollReportBuilder()
                .pollEpoch(currentEpoch)
                .healingNodes(healed)
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
     * 2. WrongEpochException is thrown. We mark as a successful response.
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
            } catch (Exception e) {
                // WrongEpochException signifies liveness of node.
                if (e.getCause() instanceof WrongEpochException) {
                    log.debug("collectResponsesAndVerifyEpochs: WrongEpochException for "
                                    + "endpoint:{}, expected epoch:{}",
                            members[i], ((WrongEpochException) e.getCause()).getCorrectEpoch());
                    responses[i] = pollIteration;
                }
            }
        }
    }

    /**
     * Aggregates all responses and checks if any node healed to rejoin the cluster.
     *
     * @param membersSet Set of unresponsive member servers
     * @return True if nodes to heal present. Else False.
     */
    private boolean isHealedNodesPresent(Set<String> membersSet) {
        // Count unsuccessful ping responses.
        Set<String> unresponsiveNodes = new HashSet<>();
        for (int i = 0; i < members.length; i++) {
            if (responses[i] == INVALID_RESPONSE) {
                unresponsiveNodes.add(members[i]);
            }
        }

        // If we receive all responses and there are no nodes to heal,
        // we can end the current poll round and exit.
        return !unresponsiveNodes.equals(membersSet);
    }
}
