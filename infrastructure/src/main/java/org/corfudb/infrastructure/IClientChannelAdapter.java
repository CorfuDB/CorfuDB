package org.corfudb.infrastructure;

import lombok.Getter;
import lombok.NonNull;
import org.corfudb.runtime.Messages.CorfuMessage;

/**
 * If Log Replication Service will rely on a custom transport protocol for inter-site communication,
 * this interface must be extended by the client-side adapter for the custom channel implementation.
 */
public abstract class IClientChannelAdapter {

    @Getter
    private final int port;

    @Getter
    private final String host;

    @Getter
    private final CustomClientRouter router;

    public IClientChannelAdapter(int port, String host, @NonNull CustomClientRouter router) {
        this.port = port;
        this.host = host;
        this.router = router;
    }

    /**
     * Send a message across the channel.
     *
     * @param msg corfu message to be sent
     */
    public abstract void send(CorfuMessage msg);

    /**
     * Adapter initialization.
     *
     * If any setup is required in advance, this will be triggered upon instantiation.
     */
    public void start() {

    }

}