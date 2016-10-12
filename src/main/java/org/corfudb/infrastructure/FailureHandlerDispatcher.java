package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.Layout;

import java.util.List;
import java.util.Set;

/**
 * The FailureHandlerDispatcher handles the trigger provided by any source
 * or policy detecting a failure in the cluster.
 * <p>
 * Created by zlokhandwala on 9/29/16.
 */
@Slf4j
public class FailureHandlerDispatcher {

    /**
     * Original layout which will be modified to remove failures.
     */
    private Layout layout = null;

    /**
     * Set of failed servers.
     */
    private Set<String> failedServers = null;

    /**
     * Takes in the existing layout and a set of failed nodes.
     * It first generates a new layout by removing the failed nodes from the
     * existing layout. It then seals the epoch to prevent any client from
     * accessing any failed nodes.
     *
     * @param corfuRuntime  Connected corfu runtime instance
     * @param layout        The current layout
     * @param failedServers Set of failed server addresses
     */
    public void dispatchHandler(CorfuRuntime corfuRuntime, Layout layout, Set<String> failedServers) {

        this.layout = layout;
        this.failedServers = failedServers;

        // Generates a new layout by removing the failed nodes from the existing layout
        generateLayout();
        // Seals the epoch with the newly generated layout.
        sealEpoch(layout);
        // We now proceed to run paxos to commit the new layout
        try {
            corfuRuntime.getLayoutView().updateLayout(layout, layout.getEpoch());
            // Check if our proposed layout got selected and committed.
            corfuRuntime.invalidateLayout();
            if (corfuRuntime.getLayoutView().getLayout().equals(layout)) {
                log.warn("Failed node removed. New Layout committed = {}", layout);
            }
        } catch (OutrankedException err) {
            log.warn("Conflict in updating layout by failureHandlerDispatcher.");
        } catch (QuorumUnreachableException err) {
            log.error("Quorum unreachable.");
            //TODO: Need to handle this condition better.
            err.printStackTrace();
        }
    }

    /**
     * Removes failed nodes from layout servers list if present.
     */
    private void removeFailedLayoutServers() {

        List<String> layoutServersList = layout.getLayoutServers();
        // Iterating over the list of layout servers in the layout
        // and removing the failed nodes addresses.
        for (int i = 0; i < layoutServersList.size(); ) {
            if (failedServers.contains(layoutServersList.get(i))) {
                layoutServersList.remove(i);
                continue;
            }
            i++;
        }
    }

    /**
     * Modifies the layout by removing the set failed nodes.
     */
    private void generateLayout() {

        // Remove failed layout servers.
        removeFailedLayoutServers();
        
        // TODO: Remove failed log unit servers and correct the chain replication
        // Throw exception if only one log unit server in a stripe and has failed.

        // TODO: Remove failed Sequencer servers.
        // Throw an error if only one seq server and has failed.
    }

    /**
     * Seals the epoch
     *
     * @param layout Current layout to be sealed
     */
    private void sealEpoch(Layout layout) {
        layout.setEpoch(layout.getEpoch() + 1);
        layout.moveServersToEpoch();
    }
}
