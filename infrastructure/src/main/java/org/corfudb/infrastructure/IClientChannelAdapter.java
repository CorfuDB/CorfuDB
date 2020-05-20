package org.corfudb.infrastructure;

import lombok.Getter;
import lombok.NonNull;
import org.corfudb.runtime.Messages.CorfuMessage;

/**
 * Client Transport Adapter.
 *
 * If Log Replication relies on a custom transport protocol for communication across servers,
 * this interface must be extended by the client-side adapter to implement a custom channel.
 *
 * @author annym 05/15/2020
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
     * If any setup is required in advance, this will be triggered upon adapter creation.
     */
    public void start() {}
}
