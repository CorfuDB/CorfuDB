package org.corfudb.integration.cluster.Harness;

import lombok.Getter;

/**
 * An abstraction for a corfu server/node.
 * <p>
 * <p>Created by maithem on 7/18/18.
 */
public class Node {

    @Getter
    final String address;

    @Getter
    String clusterAddress;

    @Getter
    final String logPath;

    final public Action shutdown;

    final public Action start;

    public Node(String address, String clusterAddress, String logPath) {
        this.address = address;
        this.clusterAddress = clusterAddress;
        this.logPath = logPath;
        this.shutdown = new ShutdownAction(this);
        this.start = new StartAction(this);
    }

    public Node(String address, String logPath) {
        this(address, null, logPath);
    }

    public String getProcessKillCommand() {
        String port = address.split(":")[1];
        return "ps aux | grep CorfuServer | grep "+ port +" |awk '{print $2}'| xargs kill -9";
    }
}
