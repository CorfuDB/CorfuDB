package org.corfudb.infrastructure;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Failure Report Model:
 * Contains a list of all records accumulated by
 * the failure detecting (polling) policy from all other nodes.
 * <p>
 * Each of these records (NodeHealthModel) contains:
 * nodeMetrics which are status metrics specific to that node.
 * Network metrics monitored by the polling node.
 * <p>
 * This failureReportModel is then passed on to the
 * Management Server which analyzes them to handle faults if any.
 * <p>
 * Created by zlokhandwala on 1/12/17.
 */
public class FailureReportModel {

    @Getter
    private List<NodeHealthModel> nodeHealthModelList;

    public FailureReportModel() {
        nodeHealthModelList = new ArrayList<>();
    }

    /**
     * This is to check if there are any failures at all.
     *
     * @return True if failures present in list.
     */
    public boolean isFailureReportModelEmpty() {
        return nodeHealthModelList.isEmpty();
    }
}
