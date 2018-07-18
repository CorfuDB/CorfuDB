package org.corfudb.integration.cluster.nodeActions;

import org.corfudb.integration.cluster.Action;
import org.corfudb.integration.cluster.Node;

public class ShutdownAction extends Action {

    public ShutdownAction(Node node) {
        this.node = node;
    }

    @Override
    public void run() throws Exception {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", node.getProcessNameRegex());
        Process p = builder.start();
        p.waitFor();
    }
}
