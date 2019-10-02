package org.corfudb.integration.cluster.Harness;

import org.corfudb.test.CorfuServerRunner;

/**
 * An action that starts a node
 * <p>Created by maithem on 7/18/18.
 */

public class StartAction extends Action {

    public StartAction(Node node) {
        this.node = node;
    }

    public void run() throws Exception {
        new CorfuServerRunner()
                .setHost(node.getAddress().split(":")[0])
                .setPort(Integer.valueOf(node.getAddress().split(":")[1]))
                .setLogPath(node.getLogPath())
                .setSingle(false)
                .runServer();
    }

}
