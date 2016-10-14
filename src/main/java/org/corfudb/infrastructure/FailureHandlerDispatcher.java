package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.Layout;

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

    private CorfuRuntime corfuRuntime;

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
        this.corfuRuntime = corfuRuntime;

        try {
            // Generates a new layout by removing the failed nodes from the existing layout
            generateLayout();

            // Seals the epoch with the newly generated layout.
            sealEpoch(this.layout);

            // We now proceed to run paxos to commit the new layout
            corfuRuntime.getLayoutView().updateLayout(this.layout, this.layout.getEpoch());

            // Check if our proposed layout got selected and committed.
            corfuRuntime.invalidateLayout();

            if (corfuRuntime.getLayoutView().getLayout().equals(this.layout)) {
                log.warn("Failed node removed. New Layout committed = {}", this.layout);
            }

        } catch (OutrankedException err) {
            log.warn("Conflict in updating layout by failureHandlerDispatcher.");
        } catch (QuorumUnreachableException err) {
            log.error("Quorum unreachable.");
            //TODO: Need to handle this condition better.
        } catch (LayoutModificationException lme) {
            // Trying to remove either all layout or sequencer or logunit servers.
            log.error(lme.getMessage());
        }
    }

    /**
     * Modifies the layout by removing the set failed nodes.
     */
    private void generateLayout() throws LayoutModificationException {

        LayoutWorkflowManager layoutManager = new LayoutWorkflowManager(layout);
        layout = layoutManager
                .removeLayoutServers(failedServers)
                .removeSequencerServers(failedServers)
                .removeLogunitServers(failedServers)
                .build();
        layout.setRuntime(corfuRuntime);
        // Throw an error if only one seq server and has failed.
        // Throw exception if only one log unit server in a stripe and has failed.
    }

    /**
     * Seals the epoch
     *
     * @param layout Current layout to be sealed
     */
    private void sealEpoch(Layout layout) {
        layout.setEpoch(layout.getEpoch() + 1);
        try {

            layout.moveServersToEpoch();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
