package org.corfudb.infrastructure.orchestrator;

import java.util.List;

/**
 *
 * A workflow is an abstract container that specifies a series of ordered steps that achieves a
 * multi-step operation. For example, adding a new node to the cluster, which requires several
 * steps to complete.
 *
 * Created by Maithem on 10/25/17.
 */

public interface Workflow {

    /**
     * Return the name of this workflow
     * @return workflow's name
     */
    String getName();

    /**
     * Returns the ordered that are associated with
     * this workflow.
     * @return List of actions
     */
    List<Action> getActions();
}
