package org.corfudb.infrastructure.management;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

/**
 * Failure Detection Policies.
 * Created by zlokhandwala on 9/29/16.
 */
public interface IFailureDetectorPolicy {

    /**
     * Executes the policy which runs the failure detecting algorithm.
     *
     * @param layout latest layout
     */
    void executePolicy(Layout layout, CorfuRuntime corfuRuntime);

    /**
     * Gets the server status from the last execution of the policy.
     *
     * @return A hash map containing servers mapped to their failure status.
     */
    PollReport getServerStatus();
}
