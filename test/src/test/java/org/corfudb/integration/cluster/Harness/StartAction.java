package org.corfudb.integration.cluster.Harness;

import org.corfudb.integration.AbstractIT;

/**
 * An action that starts a node
 * <p>Created by maithem on 7/18/18.
 */

public class StartAction extends Action {

    public StartAction(Node node) {
        this.node = node;
    }

    public void run() throws Exception {
        new AbstractIT.CorfuServerRunner()
                .setHost(node.getAddress().getHost())
                .setPort(node.getAddress().getPort())
                .setLogPath(node.getLogPath())
                .setSingle(false)
                .runServer();
    }

}
