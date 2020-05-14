package org.corfudb.infrastructure;

import lombok.Getter;
import org.corfudb.runtime.Messages.CorfuMessage;

import java.util.concurrent.CompletableFuture;

/**
 * If Log Replication relies on a custom transport protocol for inter-site communication,
 * this interface must be extended by the server-side adapter for the custom channel implementation.
 */
public abstract class IServerChannelAdapter {

    @Getter
    private final int port;

    @Getter
    private final CustomServerRouter router;

    public IServerChannelAdapter(int port, CustomServerRouter adapter) {
        this.port = port;
        this.router = adapter;
    }

    /**
     * Send message across channel.
     *
     * @param msg corfu message (protobuf definition)
     */
    public abstract void send(CorfuMessage msg);

    /**
     * Initialize adapter.
     *
     * @return Completable Future on connection start
     */
    public abstract CompletableFuture<Boolean> start();

    /**
     * Close connections or gracefully shutdown the channel.
     */
    public abstract void stop();
}
