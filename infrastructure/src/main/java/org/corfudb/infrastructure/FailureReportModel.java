package org.corfudb.infrastructure;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Failure Report Model:
 * Contains a list of node health models.
 * Each of these models contains the metrics of
 * that node required by the management server.
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
