package org.corfudb.integration.cluster.nodeActions;

import org.corfudb.integration.cluster.Action;
import org.corfudb.integration.cluster.Node;

public class RestartAction extends Action {

    public RestartAction(Node node) {
        this.node = node;
    }

    @Override
    public void run() {

    }
}
