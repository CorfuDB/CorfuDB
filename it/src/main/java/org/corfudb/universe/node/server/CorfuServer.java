package org.corfudb.universe.node.server;

import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.node.client.LocalCorfuClient;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.util.IpAddress;

import java.util.List;

/**
 * Represent a Corfu server implementation of {@link Node} used in the {@link Universe}.
 */
public interface CorfuServer extends Node, Comparable<CorfuServer> {

    @Override
    CorfuServer deploy();

    CorfuServerParams getParams();

    /**
     * Symmetrically disconnect a CorfuServer from the cluster,
     * which creates a complete partition.
     *
     * @throws NodeException thrown in case of unsuccessful disconnect.
     */
    void disconnect();

    /**
     * Symmetrically disconnect a CorfuServer from a list of other servers,
     * which creates a partial partition.
     *
     * @param servers List of servers to disconnect from
     * @throws NodeException thrown in case of unsuccessful disconnect.
     */
    void disconnect(List<CorfuServer> servers);

    /**
     * Pause a CorfuServer
     *
     * @throws NodeException thrown in case of unsuccessful pause.
     */
    void pause();

    /**
     * Restart a {@link CorfuServer}
     *
     * @throws NodeException this exception will be thrown if the node can not be restarted
     */
    void restart();

    /**
     * Start a {@link CorfuServer}
     *
     * @throws NodeException this exception will be thrown if the node can not be started
     */
    void start();

    /**
     * Reconnect a {@link CorfuServer} to the cluster
     *
     * @throws NodeException this exception will be thrown if the node can not be reconnected
     */
    void reconnect();

    /**
     * Execute a shell command on a vm
     * @param command a shell command
     * @return command output
     */
    String execute(String command);

    /**
     * Reconnect a {@link CorfuServer} to the list of servers
     *
     * @param servers List of servers to reconnect.
     * @throws NodeException this exception will be thrown if the node can not be reconnected
     */
    void reconnect(List<CorfuServer> servers);

    /**
     * Resume a {@link CorfuServer}
     *
     * @throws NodeException this exception will be thrown if the node can not be unpaused
     */
    void resume();

    IpAddress getIpAddress();

    IpAddress getNetworkInterface();

    default String getEndpoint() {
        return getNetworkInterface() + ":" + getParams().getPort();
    }

    LocalCorfuClient getLocalCorfuClient();

    /**
     * Save server logs in the server logs directory
     */
    void collectLogs();

    enum Mode {
        SINGLE, CLUSTER
    }

    enum Persistence {
        DISK, MEMORY
    }

}
