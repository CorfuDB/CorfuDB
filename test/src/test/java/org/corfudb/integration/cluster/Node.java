package org.corfudb.integration.cluster;

import lombok.Getter;
import org.corfudb.integration.cluster.nodeActions.PauseAction;
import org.corfudb.integration.cluster.nodeActions.RestartAction;
import org.corfudb.integration.cluster.nodeActions.ShutdownAction;
import org.corfudb.integration.cluster.nodeActions.StartAction;

public class Node {

    @Getter
    final String address;

    @Getter
    final String clusterAddress;

    @Getter
    final String logPath;

    final public Action shutdown;

    final public Action restart;

    final public Action pause;

    final public Action start;

    public Node(String address, String clusterAddress, String logPath) {
        this.address = address;
        this.clusterAddress = clusterAddress;
        this.logPath = logPath;
        this.shutdown = new ShutdownAction(this);
        this.restart = new RestartAction(this);
        this.pause = new PauseAction(this);
        this.start = new StartAction(this);
    }


    public String getProcessNameRegex() {
        String port = address.split(":")[1];
        return "ps aux | grep CorfuServer | grep "+ port +" |awk '{print $2}'| xargs kill -9";
    }


    // can get status by checking process?




}
