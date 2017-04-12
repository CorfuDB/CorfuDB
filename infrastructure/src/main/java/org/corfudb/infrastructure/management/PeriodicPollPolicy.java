package org.corfudb.infrastructure.management;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

/**
 * Simple Polling policy.
 * Every node polls every other node.
 *
 * <p>Failure Condition:
 * Considers failure if server a node does not respond to the ping
 * more than 2 times in a row.
 *
 * <p>Created by zlokhandwala on 9/29/16.
 */
@Slf4j
public class PeriodicPollPolicy implements IFailureDetectorPolicy {

    /**
     * list of layout servers that we monitor.
     */
    private String[] historyServers = null;
    private IClientRouter[] historyRouters = null;

    /**
     * polling history.
     */
    private int[] historyPollFailures = null;
    private long[] historyPollEpochExceptions = null;
    private ConcurrentHashMap<String, Long> historyNodeEpoch;
    private int historyPollCount = 0;
    private HashMap<String, Boolean> historyStatus = null;
    private CompletableFuture[] pollCompletableFutures = null;
    private final int pollTaskTimeout = 5000;

    /**
     * Failed Poll Limit.
     * Failed pings after which a node is considered dead.
     */
    final long FAILED_POLL_LIMIT = 3;

    /**
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers and updates the status.
     *
     * @param layout Current Layout
     */
    @Override
    public void executePolicy(Layout layout, CorfuRuntime corfuRuntime) {

        String[] allServers = layout.getAllServers().stream().toArray(String[]::new);
        // Performs setup and checks for changes in the layout to update failure count
        checkForChanges(allServers, corfuRuntime);
        // Perform polling of all servers in historyServers
        pollOnce();

    }

    /**
     * Checks for changes in the layout.
     * Fetches corfu runtime router to be able to ping a node.
     *
     * @param allServers   List of all servers in teh layout
     * @param corfuRuntime A connected corfu runtime
     */
    private void checkForChanges(String[] allServers, CorfuRuntime corfuRuntime) {

        Arrays.sort(allServers);

        if (historyServers == null || !Arrays.equals(historyServers, allServers)) {
            // Initialize the status map for the first time or if there is a change.
            if (historyStatus == null) {
                historyStatus = new HashMap<>();
            }

            log.debug("historyServers change, length = {}", allServers.length);
            historyServers = allServers;
            historyRouters = new IClientRouter[allServers.length];
            historyPollFailures = new int[allServers.length];
            historyNodeEpoch = new ConcurrentHashMap<>();
            historyPollEpochExceptions = new long[allServers.length];
            pollCompletableFutures = new CompletableFuture[allServers.length];
            for (int i = 0; i < allServers.length; i++) {
                if (!historyStatus.containsKey(allServers[i])) {
                    historyStatus.put(allServers[i], true);  // Assume it's up until we think it
                    // isn't.
                }
                historyRouters[i] = corfuRuntime.getRouterFunction.apply(allServers[i]);
                historyPollFailures[i] = 0;
                historyPollEpochExceptions[i] = 0;
            }
            historyPollCount = 0;
        } else {
            log.debug("No server list change since last poll.");
        }
    }

    /**
     * Polls all the server nodes from historyServers once.
     * If failure is detected it updates in the historyPollFailures.
     */
    private void pollOnce() {

        // Poll servers for health.  All ping activity will happen in the background.
        // We probably won't notice changes in this iteration; a future iteration will
        // eventually notice changes to historyPollFailures.
        for (int i = 0; i < historyRouters.length; i++) {
            int ii = i;  // Intermediate var just for the sake of having a final for use inside
            // the lambda below
            pollCompletableFutures[ii] = CompletableFuture.runAsync(() -> {
                try {
                    boolean pingResult = CFUtils.getUninterruptibly(historyRouters[ii]
                            .getClient(BaseClient.class)
                            .ping(), WrongEpochException.class);

                    historyPollFailures[ii] = pingResult ? 0 : historyPollFailures[ii] + 1;

                } catch (WrongEpochException wee) {
                    // If Wrong epoch exception is received, mark server as out of phase.
                    final long pingedNodeEpoch = wee.getCorrectEpoch();

                    // If pinged node has wrong epoch for the first time (see else block) we put
                    // incorrect epoch in map and increment counter.
                    // If the node is consistently on the wrong epoch we continue incrementing
                    // the counter.
                    if (pingedNodeEpoch == historyNodeEpoch
                            .getOrDefault(historyServers[ii], -1L)) {

                        historyPollEpochExceptions[ii]++;
                    } else {
                        historyNodeEpoch.put(historyServers[ii], pingedNodeEpoch);
                        historyPollEpochExceptions[ii] = 0;
                    }
                } catch (Exception e) {
                    // For any other exception mark node as failure.
                    log.debug("Ping failed for {}. Cause : {}", historyServers[ii], e);
                    historyPollFailures[ii]++;
                }
            });
        }
        historyPollCount++;
    }

    /**
     * Gets the server status from the last poll.
     * Reports failures or partially sealed servers.
     * The reports reflect these abnormalities after
     * at least 'FAILED_POLL_LIMIT' polls.
     *
     * @return A map of failed server nodes and their status.
     */
    @Override
    public PollReport getServerStatus() {

        Set<String> failingNodes = new HashSet<>();
        HashMap<String, Long> outOfPhaseEpochNodes = new HashMap<>();

        if (historyPollCount > 3) {
            boolean isUp;
            boolean isPartialSeal;

            // Simple failure detector: Is there a change in health?
            for (int i = 0; i < historyServers.length; i++) {
                // TODO: Be a bit smarter than 'more than 2 failures in a row'

                // Block until we complete the previous polling round.
                try {
                    pollCompletableFutures[i].get(pollTaskTimeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    log.error("Error in polling task for server {} : {}", historyServers[i], e);
                    pollCompletableFutures[i].cancel(true);
                    // Assuming server is unresponsive if ping task stuck or interrupted or
                    // throws exception.
                    historyPollFailures[i]++;
                }

                // The count remains within the interval 0 <= failureCount <= FAILED_POLL_LIMIT(3)
                isUp = !(historyPollFailures[i] >= FAILED_POLL_LIMIT);
                // If the seal or paxos was not completed the node is stuck with an older epoch
                // or has been sealed but layout server has not received latest layout.
                isPartialSeal = historyPollEpochExceptions[i] >= FAILED_POLL_LIMIT;

                // Toggle if server was up and now not responding
                if (!isUp) {
                    log.debug("Change of status: " + historyServers[i] + " "
                            + historyStatus.get(historyServers[i]) + " -> " + isUp);
                    failingNodes.add(historyServers[i]);
                    historyStatus.put(historyServers[i], isUp);
                    historyPollFailures[i]--;
                } else if (!historyStatus.get(historyServers[i])) {
                    if (historyPollFailures[i] > 0) {
                        if (--historyPollFailures[i] == 0) {
                            log.debug("Change of status: " + historyServers[i] + " "
                                    + historyStatus.get(historyServers[i]) + " -> " + true);
                            historyStatus.put(historyServers[i], true);
                        } else {
                            // Server still down
                            failingNodes.add(historyServers[i]);
                        }
                    }
                }
                if (isPartialSeal) {
                    // Mark server as out of phased epoch. NOTE: This is not the same as
                    // marking it as failed.
                    outOfPhaseEpochNodes
                            .put(historyServers[i], historyNodeEpoch.get(historyServers[i]));
                    // Reset epoch exception value.
                    historyPollEpochExceptions[i] = 0;
                }
            }

            if (!outOfPhaseEpochNodes.isEmpty()) {
                // Reset all epoch exceptions as all endpoints will be sealed.
                historyPollEpochExceptions = new long[historyRouters.length];
            }
        }
        return new PollReport.PollReportBuilder()
                .setFailingNodes(failingNodes)
                .setOutOfPhaseEpochNodes(outOfPhaseEpochNodes)
                .build();
    }
}
