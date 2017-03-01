package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.Layout;

import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * The FailureHandlerDispatcher handles the trigger provided by any source
 * or policy detecting a failure in the cluster.
 * <p>
 * Created by zlokhandwala on 11/18/16.
 */
@Slf4j
public class FailureHandlerDispatcher {

    /**
     * Takes in the existing layout and a set of failed nodes.
     * It first generates a new layout by removing the failed nodes from the existing layout.
     * It then seals the epoch to prevent any client from accessing the stale layout.
     * Finally we run paxos to update all servers with the new layout.
     *
     * @param currentLayout The current layout
     * @param corfuRuntime  Connected corfu runtime instance
     * @param failedServers Set of failed server addresses
     */
    public void dispatchHandler(IFailureHandlerPolicy failureHandlerPolicy, Layout currentLayout, CorfuRuntime corfuRuntime, Set<String> failedServers) {

        try {

            // Generates a new layout by removing the failed nodes from the existing layout
            Layout newLayout = failureHandlerPolicy.generateLayout(currentLayout, corfuRuntime, failedServers);

            // Seals and increments the epoch.
            currentLayout.setRuntime(corfuRuntime);
            sealEpoch(currentLayout);

            // Reconfigure servers if required
            reconfigureServers(corfuRuntime, currentLayout, newLayout);

            // Attempts to update all the layout servers with the modified layout.
            corfuRuntime.getLayoutView().updateLayout(newLayout, newLayout.getEpoch());

            // Check if our proposed layout got selected and committed.
            corfuRuntime.invalidateLayout();
            if (corfuRuntime.getLayoutView().getLayout().equals(newLayout)) {
                log.info("Failed node removed. New Layout committed = {}", newLayout);
            }

        } catch (OutrankedException oe) {
            log.error("Conflict in updating layout by failureHandlerDispatcher: {}", oe);
        } catch (Exception e) {
            log.error("Error: dispatchHandler: {}", e);
        }
    }

    /**
     * Seals the epoch
     * Set local epoch and then attempt to move all servers to new epoch
     *
     * @param layout Layout to be sealed
     */
    private void sealEpoch(Layout layout) throws QuorumUnreachableException {
        layout.setEpoch(layout.getEpoch() + 1);
        layout.moveServersToEpoch();
    }

    /**
     * Reconfigures the servers in the new layout if reconfiguration required.
     *
     * @param runtime        Runtime to reconfigure new servers.
     * @param originalLayout Current layout to get the latest state of servers.
     * @param newLayout      New Layout to be reconfigured.
     */
    private void reconfigureServers(CorfuRuntime runtime, Layout originalLayout, Layout newLayout) {

        // Reconfigure the primary Sequencer Server if changed.
        reconfigureSequencerServers(runtime, originalLayout, newLayout);

        // TODO: Reconfigure log units if new log unit added.
    }

    /**
     * Reconfigures the sequencer.
     * If the primary sequencer has changed in the new layout,
     * the global tail of the log units are queried and used to set
     * the initial token of the new primary sequencer.
     *
     * @param runtime           Runtime to reconfigure new servers.
     * @param originalLayout    Current layout to get the latest state of servers.
     * @param newLayout         New Layout to be reconfigured.
     */
    private void reconfigureSequencerServers(CorfuRuntime runtime, Layout originalLayout, Layout newLayout) {

        // Reconfigure Primary Sequencer if required
        if (!originalLayout.getSequencers().get(0).equals(newLayout.getSequencers().get(0))) {
            long maxTokenRequested = 0;
            for (Layout.LayoutSegment segment : originalLayout.getSegments()) {
                // Query the tail of every log unit in every stripe.
                for (Layout.LayoutStripe stripe : segment.getStripes()) {
                    for (String logServer : stripe.getLogServers()) {
                        try {
                            long tail = runtime.getRouter(logServer).getClient(LogUnitClient.class).getTail().get();
                            if (tail != 0) {
                                maxTokenRequested = maxTokenRequested > tail ? maxTokenRequested : tail;
                            }
                        } catch (Exception e) {
                            log.error("Exception while fetching log unit tail : {}", e);
                        }
                    }
                }
            }
            try {
                // Configuring the new sequencer.
                newLayout.getSequencer(0).reset(maxTokenRequested + 1).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}