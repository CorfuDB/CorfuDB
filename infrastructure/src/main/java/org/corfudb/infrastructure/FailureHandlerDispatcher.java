package org.corfudb.infrastructure;

import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.IFailureHandlerPolicy;
import org.corfudb.runtime.view.Layout;

/**
 * The FailureHandlerDispatcher handles the trigger provided by any source
 * or policy detecting a failure in the cluster.
 *
 * <p>Created by zlokhandwala on 11/18/16.
 */
@Slf4j
public class FailureHandlerDispatcher {

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
            e.printStackTrace();
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
    public void dispatchHandler(IFailureHandlerPolicy failureHandlerPolicy, Layout currentLayout,
                                CorfuRuntime corfuRuntime, Set<String> failedServers) {
        try {
            corfuRuntime.getLayoutManagementView().handleFailure(failureHandlerPolicy,
                    currentLayout, failedServers);
        } catch (Exception e) {
            log.error("Error: dispatchHandler: {}", e);
        }
    }
}