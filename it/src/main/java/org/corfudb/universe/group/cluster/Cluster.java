package org.corfudb.universe.group.cluster;

import org.corfudb.universe.group.Group;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.node.Node;

public interface Cluster<T extends Node, G extends GroupParams> extends Group<T, G> {

    /**
     * Bootstrap a {@link Cluster}
     */
    void bootstrap();

    enum ClusterType {
        CORFU_CLUSTER, SUPPORT_CLUSTER
    }
}
