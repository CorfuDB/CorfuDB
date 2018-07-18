package org.corfudb.integration.cluster.nodeActions;

import org.corfudb.integration.AbstractIT;
import org.corfudb.integration.cluster.Action;
import org.corfudb.integration.cluster.Node;

public class StartAction extends Action {

    public StartAction(Node node) {
        this.node = node;
    }

    public void run() throws Exception {
        new AbstractIT.CorfuServerRunner()
                .setHost(node.getAddress().split(":")[0])
                .setPort(Integer.valueOf(node.getAddress().split(":")[1]))
                .setLogPath(node.getLogPath())
                .setSingle(false)
                .runServer();
    }

}
