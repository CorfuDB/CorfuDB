package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;

import java.util.Set;

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
            sealEpoch(newLayout);
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
     * @param layout Current layout to be sealed
     */
    private void sealEpoch(Layout layout) {
        layout.setEpoch(layout.getEpoch() + 1);
        layout.moveServersToEpoch();
    }
}