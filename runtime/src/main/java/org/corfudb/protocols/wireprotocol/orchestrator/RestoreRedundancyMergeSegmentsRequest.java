package org.corfudb.protocols.wireprotocol.orchestrator;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.RESTORE_REDUNDANCY_MERGE_SEGMENTS;

import java.nio.charset.StandardCharsets;

import lombok.Getter;

/**
 * An orchestrator request message to restore redundancy and merge all segments in the cluster.
 * Created by Zeeshan on 2019-02-06.
 */
public class RestoreRedundancyMergeSegmentsRequest implements CreateRequest {

    /**
     * Endpoint to restore redundancy to. Data needs to be transferred to this node.
     */
    @Getter
    private final String endpoint;

    public RestoreRedundancyMergeSegmentsRequest(String endpoint) {
        this.endpoint = endpoint;
    }

    public RestoreRedundancyMergeSegmentsRequest(byte[] buf) {
        endpoint = new String(buf, StandardCharsets.UTF_8);
    }

    @Override
    public OrchestratorRequestType getType() {
        return RESTORE_REDUNDANCY_MERGE_SEGMENTS;
    }

    @Override
    public byte[] getSerialized() {
        return endpoint.getBytes(StandardCharsets.UTF_8);
    }
}
