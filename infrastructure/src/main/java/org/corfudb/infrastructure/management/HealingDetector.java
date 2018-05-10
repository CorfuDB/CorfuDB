package org.corfudb.infrastructure.management;

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
import org.corfudb.util.Sleep;

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

    /**
     * Triggered on every poll.
     * Sets up the unresponsive servers list. (All servers in the unresponsiveServers list in
     * the layout)
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers and generates the report.
     *
     * @param layout Current Layout
     */
    public PollReport poll(@Nonnull Layout layout,
                           @Nonnull CorfuRuntime corfuRuntime) {

        // Get all unresponsive servers.
        final List<String> members = new ArrayList<>(layout.getUnresponsiveServers());

        log.debug("Unresponsive members to poll, {}", members);
        Map<String, IClientRouter> routerMap = new HashMap<>();

        members.forEach(member -> {
            try {
                IClientRouter router = corfuRuntime.getRouter(member);
                router.setTimeoutResponse(detectionPeriodDuration);
                routerMap.put(member, router);
            } catch (NetworkException ne) {
                log.debug("Error creating router for {}", member);
            }
        });

        // Perform polling of all unresponsive servers.
        return new PollReport.PollReportBuilder()
                .pollEpoch(layout.getEpoch())
                .healingNodes(pollRound(members, routerMap, layout.getEpoch()))
                .build();
    }

    /**
     * Each PollRound executes "detectionThreshold" number of iterations and generates the
     * poll report.
     *
     * @return Poll Report with detected healed nodes.
     */
    private Set<String> pollRound(List<String> members,
                                  Map<String, IClientRouter> routerMap,
                                  long epoch) {

        Map<String, Integer> pollResultMap = new HashMap<>();

        // In each iteration we poll all the servers in the members list.
        for (int iteration = 0; iteration < detectionThreshold; iteration++) {

            // Ping all nodes and await their responses.
            Map<String, CompletableFuture<NodeView>> pollCompletableFutures =
                    pollOnceAsync(members, routerMap, epoch);

            // Collect all poll responses.
            Set<String> responses = collectResponsesAndVerifyEpochs(members,
                    pollCompletableFutures);

            // Count unsuccessful ping responses.
            // If we receive no responses and there are no nodes to heal,
            // we can end the current poll round and exit.
            if (responses.isEmpty()) {
                break;
            }

            final int pollIteration = iteration;
            responses.forEach(s -> pollResultMap.put(s, pollIteration));

            Sleep.MILLISECONDS.sleepUninterruptibly(interIterationInterval);
        }

        // Check all responses and report all healed nodes.
        return members.stream()
                .filter(s -> pollResultMap.get(s) != null
                        && pollResultMap.get(s) == (detectionThreshold - 1))
                .collect(Collectors.toSet());
    }

    /**
     * Poll all members servers once asynchronously and return their futures.
     *
     * @param members   All unresponsive members.
     * @param routerMap Map of members corresponding to their client routers.
     * @param epoch     Current epoch for the polling round to stamp the ping messages.
     * @return Map of completable futures generated on their respective pings.
     */
    private Map<String, CompletableFuture<NodeView>> pollOnceAsync(List<String> members,
                                                                   Map<String, IClientRouter>
                                                                           routerMap,
                                                                   final long epoch) {

        // Poll servers for health.  All ping activity will happen in the background.
        final Map<String, CompletableFuture<NodeView>> pollCompletableFutures = new HashMap<>();
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
     * 2. WrongEpochException is thrown. We mark as a successful response.
     * 3. Other Exception is thrown. We do nothing. (The response[i] in this case is left behind.)
     *
     * @param members                All unresponsive members.
     * @param pollCompletableFutures Map of all members corresponding to their ping completable
     *                               futures.
     * @return Set of all responsive nodes.
     */
    private Set<String> collectResponsesAndVerifyEpochs(List<String> members,
                                                        Map<String, CompletableFuture<NodeView>>
                                                                pollCompletableFutures) {
        // Collect responses and increment response counters for successful pings.
        Set<String> responses = new HashSet<>();
        members.forEach(s -> {
            try {
                pollCompletableFutures.get(s).get();
                responses.add(s);
            } catch (Exception e) {
                // WrongEpochException signifies liveness of node.
                if (e.getCause() instanceof WrongEpochException) {
                    log.debug("collectResponsesAndVerifyEpochs: WrongEpochException for "
                                    + "endpoint:{}, expected epoch:{}",
                            s, ((WrongEpochException) e.getCause()).getCorrectEpoch());
                    responses.add(s);
                }
            }
        });
        return responses;
    }
}
