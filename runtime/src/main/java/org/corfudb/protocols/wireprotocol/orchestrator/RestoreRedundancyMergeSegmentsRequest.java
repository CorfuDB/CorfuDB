package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.RESTORE_REDUNDANCY_MERGE_SEGMENTS;

/**
 * An orchestrator request message to restore redundancy and merge all segments in the cluster.
 * Created by Zeeshan on 2019-02-06.
 */
public class RestoreRedundancyMergeSegmentsRequest implements Request {

    /**
     * Endpoint to restore redundancy to. Data needs to be transferred to this node.
     */
    @Getter
    private final String endpoint;

    public RestoreRedundancyMergeSegmentsRequest(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public OrchestratorRequestType getType() {
        return RESTORE_REDUNDANCY_MERGE_SEGMENTS;
    }
}
