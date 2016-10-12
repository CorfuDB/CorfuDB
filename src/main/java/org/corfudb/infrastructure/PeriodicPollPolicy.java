package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.view.Layout;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

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

    private CorfuRuntime corfuRuntime;
    /**
     * list of layout servers that we monitor for ping'ability.
     */
    private String[] historyServers = null;
    private IClientRouter[] historyRouters = null;

    /**
     * polling history
     */
    private int[] historyPollFailures = null;
    private int historyPollCount = 0;
    private HashMap<String, Boolean> historyStatus = null;

    /**
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers and updates the status.
     *
     * @param layout Current Layout
     */
    @Override
    public void executePolicy(Layout layout, CorfuRuntime corfuRuntime) {

        this.corfuRuntime = corfuRuntime;
        String[] allServers = layout.getAllServers().stream().toArray(String[]::new);
        // Performs setup and checks for changes in the layout to update failure count
        checkForChanges(allServers);
        // Perform polling of all servers in historyServers
        pollOnce();

    }

    /**
     * Checks for changes in the layout.
     * Fetches corfu runtime router to be able to ping a node.
     *
     * @param allServers List of all servers in teh layout
     */
    private void checkForChanges(String[] allServers) {

        Arrays.sort(allServers);

        if (historyServers == null || !Arrays.equals(historyServers, allServers)) {
            if (historyStatus == null) {
                historyStatus = new HashMap<>();
            }

            log.debug("historyServers change, length = {}", allServers.length);
            historyServers = allServers;
            historyRouters = new IClientRouter[allServers.length];
            historyPollFailures = new int[allServers.length];
            for (int i = 0; i < allServers.length; i++) {
                if (!historyStatus.containsKey(allServers[i])) {
                    historyStatus.put(allServers[i], true);  // Assume it's up until we think it isn't.
                }
                historyRouters[i] = corfuRuntime.getRouterFunction.apply(allServers[i]);
                historyRouters[i].setTimeoutConnect(50);
                historyRouters[i].setTimeoutRetry(200);
                historyRouters[i].setTimeoutResponse(1000);
                historyRouters[i].start();
                historyPollFailures[i] = 0;
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
            CompletableFuture.runAsync(() -> {
                // Any changes that we make to historyPollFailures here can possibly
                // race with other async CFs that were launched in earlier/later CFs.
                // We don't care if an increment gets clobbered by another increment:
                //     being off by one isn't a big problem.
                // We don't care if a reset to zero gets clobbered by an increment:
                //     if the endpoint is really pingable, then a later reset to zero
                //     will succeed, probably.
                try {
                    CompletableFuture<Boolean> cf = historyRouters[ii].getClient(BaseClient.class).ping();
                    cf.exceptionally(e -> {
                        log.debug(historyServers[ii] + " exception " + e);
                        historyPollFailures[ii]++;
                        return false;
                    });
                    cf.thenAccept((pingResult) -> {
                        if (pingResult) {
                            historyPollFailures[ii] = 0;
                        } else {
                            historyPollFailures[ii]++;
                        }
                    });

                } catch (Exception e) {
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
    public HashMap<String, Boolean> getServerStatus() {

        HashMap<String, Boolean> status_change = new HashMap<>();
        if (historyPollCount > 3) {
            Boolean is_up;

            // Simple failure detector: Is there a change in health?
            for (int i = 0; i < historyServers.length; i++) {
                // TODO: Be a bit smarter than 'more than 2 failures in a row'
                is_up = !(historyPollFailures[i] > 2);
                if (is_up != historyStatus.get(historyServers[i])) {
                    log.debug("Change of status: " + historyServers[i] + " " +
                            historyStatus.get(historyServers[i]) + " -> " + is_up);
                    status_change.put(historyServers[i], is_up);

                    // Resetting the failure counter.
                    historyPollFailures[i] = 0;
                }
            }

        }
        return status_change;
    }
}
