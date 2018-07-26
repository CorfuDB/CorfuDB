package org.corfudb.integration.cluster.Harness;

/**
 * An action that shuts down a node.
 * <p>Created by maithem on 7/18/18.
 */

public class ShutdownAction extends Action {

    public ShutdownAction(Node node) {
        this.node = node;
    }

    @Override
    public void run() throws Exception {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", node.getProcessKillCommand());
        Process p = builder.start();
        p.waitFor();
    }
}
