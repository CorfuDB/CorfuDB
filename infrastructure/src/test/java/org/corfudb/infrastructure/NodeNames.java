package org.corfudb.infrastructure;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Common node names for testing purposes
 */
public class NodeNames {
    public static final String A = NodeName.a.name();
    public static final String B = NodeName.b.name();
    public static final String C = NodeName.c.name();

    private NodeNames() {
        //prevent creating instances
    }

    public static final String NOT_IN_CLUSTER_NODE = "not-in-cluster-node" + new Random().nextInt();

    public static final List<NodeName> NODE_NAMES = Arrays.asList(NodeName.values());

    public static final List<String> ABC_NODES = Arrays.asList(A, B, C);

    public enum NodeName {
        a, b, c, d, e, f, g, h, i, j, k
    }

}
