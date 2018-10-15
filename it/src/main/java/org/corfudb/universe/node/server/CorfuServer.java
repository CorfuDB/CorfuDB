package org.corfudb.universe.node.server;

import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.universe.Universe;

/**
 * Represent a Corfu server implementation of {@link Node} used in the {@link Universe}.
 */
public interface CorfuServer extends Node {

    @Override
    CorfuServer deploy();

    CorfuServerParams getParams();

    /**
     * Disconnect a CorfuServer from the network
     *
     * @throws NodeException thrown in case of unsuccessful disconnect.
     */
    void disconnect();

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
     * Reconnect a {@link CorfuServer} to the network
     *
     * @throws NodeException this exception will be thrown if the node can not be reconnected
     */
    void reconnect();

    /**
     * Resume a {@link CorfuServer}
     *
     * @throws NodeException this exception will be thrown if the node can not be unpaused
     */
    void resume();

    String getIpAddress();

    String getNetworkInterface();

    default String getEndpoint() {
        return getNetworkInterface() + ":" + getParams().getPort();
    }

    enum Mode {
        SINGLE, CLUSTER
    }

    enum Persistence {
        DISK, MEMORY
    }

}
