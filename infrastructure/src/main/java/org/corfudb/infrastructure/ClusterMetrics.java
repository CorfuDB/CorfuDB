package org.corfudb.infrastructure;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * ClusterMetrics:
 * Contains a list of all records accumulated by
 * the polling component from all other nodes.
 * <p>
 * Each of these records (NodeHealth) contains:
 *  NodeMetrics which are status metrics specific to that node.
 *  Network metrics monitored by the polling node.
 * <p>
 * +--------------------+   +--------------------------+
 * |NodeMetricPollPolicy+-->+     ClusterMetrics       |
 * +--------------------+   | +---------------+        |
 *                          | |  NodeHealth   |        |
 *                          | | +-----------+ +--+     |
 *                          | | |NodeMetrics| |  |     |
 *                          | | +-----------+ |  +--+  |
 *                          | +---------------+  |  |  |
 *                          |    |               |  |  |
 *                          |    +--+------------+  |  |
 *                          |       |               |  |
 *                          |       +---------------+  |
 *                          +--------------------------+
 * <p>
 * This ClusterMetrics is then passed on to the
 * Management Server which analyzes them to handle faults if any.
 * <p>
 * Created by zlokhandwala on 1/12/17.
 */
public class ClusterMetrics {

    @Getter
    private List<NodeHealth> nodeHealthList;

    public ClusterMetrics() {
        nodeHealthList = new ArrayList<>();
    }

    /**
     * This is to check if there are any failures at all.
     *
     * @return True if failures present in list.
     */
    public boolean isFailureReportModelEmpty() {
        return nodeHealthList.isEmpty();
    }
}
