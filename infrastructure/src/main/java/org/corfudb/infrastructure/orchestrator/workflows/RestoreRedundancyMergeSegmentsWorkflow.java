package org.corfudb.infrastructure.orchestrator.workflows;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.IWorkflow;
import org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments;
import org.corfudb.infrastructure.redundancy.RedundancyCalculator;
import org.corfudb.protocols.wireprotocol.orchestrator.RestoreRedundancyMergeSegmentsRequest;

import java.util.List;
import java.util.UUID;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.RESTORE_REDUNDANCY_MERGE_SEGMENTS;

/**
 * A definition of a workflow that merges all the segments in the layout.
 * This workflow compares every two consequent segments and restores the redundancy in all nodes present in both the
 * segments. After the state transfer, the workflow merges the segments. This is carried out for all segments until
 * the layout has only one segment left.
 * Created by Zeeshan on 2019-02-06.
 */
@Slf4j
public class RestoreRedundancyMergeSegmentsWorkflow implements IWorkflow {


    @Getter
    private final UUID id;

    @Getter
    private final List<Action> actions;

    /**
     * Creates a new merge segments workflow from a request.
     *
     * @param request request to restore redundancy and merge a segment.
     */
    public RestoreRedundancyMergeSegmentsWorkflow(
            RestoreRedundancyMergeSegmentsRequest request) {
        this.id = UUID.randomUUID();
        this.actions = ImmutableList.of(
                RestoreRedundancyMergeSegments.builder()
                        .currentNode(request.getEndpoint())
                        .redundancyCalculator(new RedundancyCalculator(request.getEndpoint()))
                        .build());
    }

    @Override
    public String getName() {
        return RESTORE_REDUNDANCY_MERGE_SEGMENTS.toString();
    }
}
