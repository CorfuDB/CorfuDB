package org.corfudb.integration.cluster.nodeActions;

import org.corfudb.integration.cluster.Action;
import org.corfudb.integration.cluster.Node;

public class PauseAction extends Action {

    public PauseAction(Node node) {
        this.node = node;
    }

    @Override
    public void run() {

    }
}
