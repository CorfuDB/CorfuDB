package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The type of requests that can be made to the Orchestrator Service.
 *
 * @author Maithem
 */
@AllArgsConstructor
public enum OrchestratorRequestType {

    /**
     * Query a workflow id
     */
    QUERY(0),

    /**
     * Add a new node to the cluster
     */
    ADD_NODE(1),

    /**
     * Remove a node from the cluster
     */
    REMOVE_NODE(2),

    /**
     * Heal an existing node in the cluster
     */
    HEAL_NODE(3),

    /**
     * Force remove a node from the cluster
     */
    FORCE_REMOVE_NODE(4),

    /**
     * Restore redundancy and merge segments in the layout.
     */
    RESTORE_REDUNDANCY_MERGE_SEGMENTS(5);

    @Getter
    public final int type;
}
