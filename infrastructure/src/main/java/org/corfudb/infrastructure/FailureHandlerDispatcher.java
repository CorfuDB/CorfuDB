package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LayoutModificationException;
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
    public void dispatchHandler(Layout currentLayout, CorfuRuntime corfuRuntime, Set<String> failedServers) {

        try {

            // Generates a new layout by removing the failed nodes from the existing layout
            Layout newLayout = generateLayout(currentLayout, corfuRuntime, failedServers);
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
     * Modifies the layout by removing the set failed nodes.
     *
     * @param originalLayout Original Layout which needs to be modified.
     * @param corfuRuntime   Connected runtime to attach to the new layout.
     * @param failedServers  Set of all failed/defected servers.
     * @return The new and modified layout.
     * @throws LayoutModificationException Thrown if attempt to create an invalid layout.
     * @throws CloneNotSupportedException  Clone not supported for layout.
     */
    private Layout generateLayout(Layout originalLayout, CorfuRuntime corfuRuntime, Set<String> failedServers)
            throws LayoutModificationException, CloneNotSupportedException {

        LayoutWorkflowManager layoutManager = new LayoutWorkflowManager(originalLayout);
        Layout newLayout = layoutManager
                .removeLayoutServers(failedServers)
                .removeSequencerServers(failedServers)
                .removeLogunitServers(failedServers)
                .build();
        newLayout.setRuntime(corfuRuntime);
        return newLayout;
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