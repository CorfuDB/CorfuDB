package org.corfudb.infrastructure.management;

import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.IReconfigurationHandlerPolicy;
import org.corfudb.runtime.view.Layout;

/**
 * The ReconfigurationEventHandler handles the trigger provided by any source
 * or policy detecting a failure or healing in the cluster. It performs the following functions:
 * Cluster recovery:    Recovers the cluster on trigger at startup.
 * Handle failures:     Handles any failures in the layout based on the desired policy.
 * Handle healing:      Handles healing of unresponsive marked nodes which are now responsive.
 *
 * <p>Created by zlokhandwala on 11/18/16.
 */
@Slf4j
public class ReconfigurationEventHandler {

    /**
     * Recover cluster from layout.
     *
     * @param recoveryLayout Layout to use to recover
     * @param corfuRuntime   Connected runtime
     * @return True if the cluster was recovered, False otherwise
     */
    public boolean recoverCluster(Layout recoveryLayout, CorfuRuntime corfuRuntime) {

        try {
            corfuRuntime.getLayoutManagementView().recoverCluster(recoveryLayout);
        } catch (Exception e) {
            log.error("Error: recovery: {}", e);
            return false;
        }
        return true;
    }

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
    public boolean handleFailure(IReconfigurationHandlerPolicy failureHandlerPolicy,
                                 Layout currentLayout,
                                 CorfuRuntime corfuRuntime,
                                 Set<String> failedServers) {
        try {
            corfuRuntime.getLayoutManagementView().handleFailure(failureHandlerPolicy,
                    currentLayout, failedServers);
            return true;
        } catch (Exception e) {
            log.error("Error: handleFailure: {}", e);
            return false;
        }
    }

    /**
     * Takes in the existing layout and a set of healed nodes.
     * It first generates a new layout by adding the healed nodes to the existing layout.
     * It then seals the epoch to prevent any client from accessing the stale layout.
     * Finally we run paxos to update all servers with the new layout.
     *
     * @param currentLayout The current layout
     * @param corfuRuntime  Connected corfu runtime instance
     * @param healedServers Set of healed server addresses
     */
    public boolean handleHealing(IReconfigurationHandlerPolicy failureHandlerPolicy,
                                 Layout currentLayout,
                                 CorfuRuntime corfuRuntime,
                                 Set<String> healedServers) {
        try {
            corfuRuntime.getLayoutManagementView().handleHealing(failureHandlerPolicy,
                    currentLayout, healedServers);
            return true;
        } catch (Exception e) {
            log.error("Error: handleHealing: {}", e);
            return false;
        }
    }
}