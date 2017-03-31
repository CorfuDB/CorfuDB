package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Simple Polling policy.
 * Every node polls every other node.
 * <p>
 * Failure Condition:
 * Considers failure if server a node does not respond to the ping
 * more than 2 times in a row.
 * <p>
 * Created by zlokhandwala on 9/29/16.
 */
@Slf4j
public class PeriodicPollPolicy implements IFailureDetectorPolicy {

    /**
     * list of layout servers that we monitor.
     */
    private String[] historyServers = null;
    private IClientRouter[] historyRouters = null;

    /**
     * polling history
     */
    private int[] historyPollFailures = null;
    private long[] historyPollEpochExceptions = null;
    private int historyPollCount = 0;
    private HashMap<String, Boolean> historyStatus = null;
    private CompletableFuture[] pollCompletableFutures = null;
    private final int pollTaskTimeout = 5000;

    /**
     * Failed Poll Limit.
     * Failed pings after which a node is considered dead.
     */
    long failedPollLimit = 3;

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
            historyPollEpochExceptions = new long[allServers.length];
            pollCompletableFutures = new CompletableFuture[allServers.length];
            for (int i = 0; i < allServers.length; i++) {
                if (!historyStatus.containsKey(allServers[i])) {
                    historyStatus.put(allServers[i], true);  // Assume it's up until we think it isn't.
                }
                historyRouters[i] = corfuRuntime.getRouterFunction.apply(allServers[i]);
                historyRouters[i].start();
                historyPollFailures[i] = 0;
                historyPollEpochExceptions[i] = -1;
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
            int ii = i;  // Intermediate var just for the sake of having a final for use inside the lambda below
            pollCompletableFutures[ii] = CompletableFuture.runAsync(() -> {
                try {
                    boolean pingResult = historyRouters[ii].getClient(BaseClient.class).ping().get();
                    historyPollFailures[ii] = pingResult ? 0 : historyPollFailures[ii] + 1;
                } catch (Exception e ) {
                    // If Wrong epoch exception is received, mark server as failed.
                    if (e.getCause() instanceof WrongEpochException) {
                        historyPollEpochExceptions[ii] = ((WrongEpochException) e.getCause()).getCorrectEpoch();
                    }
                    log.debug("Ping failed for " + historyServers[ii] + " with " + e);
                    historyPollFailures[ii]++;
                }
            });
        }
        historyPollCount++;
    }

    /**
     * Gets the server status from the last poll.
     * A failure is detected after at least 2 polls.
     *
     * @return A map of failed server nodes and their status.
     */
    @Override
    public PollReport getServerStatus() {

        Set<String> failingNodes = new HashSet<>();
        HashMap<String, Long> outOfPhaseEpochNodes = new HashMap<>();

        if (historyPollCount > 3) {
            Boolean is_up;

            // Simple failure detector: Is there a change in health?
            for (int i = 0; i < historyServers.length; i++) {
                // TODO: Be a bit smarter than 'more than 2 failures in a row'

                // Block until we complete the previous polling round.
                try {
                    pollCompletableFutures[i].get(pollTaskTimeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    log.error("Error in polling task for server {} : {}", historyServers[i], e);
                    pollCompletableFutures[i].cancel(true);
                    // Assuming server is unresponsive if ping task stuck or interrupted or throws exception.
                    historyPollFailures[i]++;
                }

                // The count remains within the interval 0 <= failureCount <= failedPollLimit(3)
                is_up = !(historyPollFailures[i] >= failedPollLimit);
                // Toggle if server was up and now not responding
                if (!is_up) {
                    log.debug("Change of status: " + historyServers[i] + " " +
                            historyStatus.get(historyServers[i]) + " -> " + is_up);
                    failingNodes.add(historyServers[i]);
                    historyStatus.put(historyServers[i], is_up);
                    historyPollFailures[i]--;
                } else if (!historyStatus.get(historyServers[i])) {
                    // If server was down but now responsive so wait till reaches lower watermark (0).
                    if (historyPollFailures[i] > 0) {
                        if (--historyPollFailures[i] == 0) {
                            log.debug("Change of status: " + historyServers[i] + " " +
                                    historyStatus.get(historyServers[i]) + " -> " + true);
                            historyStatus.put(historyServers[i], true);
                        } else {
                            // Server still down
                            failingNodes.add(historyServers[i]);
                        }
                    }
                }
                if (historyPollEpochExceptions[i] != -1) {
                    outOfPhaseEpochNodes.put(historyServers[i], historyPollEpochExceptions[i]);
                    // Reset epoch exception value.
                    historyPollEpochExceptions[i] = -1;
                }
            }
        }
        return new PollReport.PollReportBuilder()
                .setFailingNodes(failingNodes)
                .setOutOfPhaseEpochNodes(outOfPhaseEpochNodes)
                .build();
    }
}
