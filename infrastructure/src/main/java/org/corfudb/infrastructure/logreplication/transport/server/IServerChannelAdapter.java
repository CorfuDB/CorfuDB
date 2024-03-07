package org.corfudb.infrastructure.logreplication.transport.server;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.infrastructure.logreplication.transport.IChannelContext;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.concurrent.CompletableFuture;

/**
 * Server Transport Adapter.
 *
 * If Log Replication relies on a custom transport protocol for communication across servers,
 * this interface must be extended by the server-side adapter to implement a custom channel.
 *
 * @author annym 05/15/2020
 */
public abstract class IServerChannelAdapter {

    @Getter
    private final LogReplicationServerRouter router;

    @Getter
    private final ServerContext serverContext;

    @Getter
    @Setter
    private IChannelContext channelContext;

    public IServerChannelAdapter(ServerContext serverContext, LogReplicationServerRouter adapter) {
        this.serverContext = serverContext;
        this.router = adapter;
    }

    /**
     * Send message across channel.
     *
     * @param msg corfu message (protoBuf definition)
     */
    public abstract void send(CorfuMessage.ResponseMsg msg);

    /**
     * Receive a message from Client.
     *
     * @param msg received corfu message
     */
    public void receive(CorfuMessage.RequestMsg msg) {
        getRouter().receive(msg);
    }

    /**
     * Initialize adapter.
     *
     * @return Completable Future on connection start
     */
    public abstract CompletableFuture<Boolean> start();

    /**
     * Close connections or gracefully shutdown the channel.
     */
    public void stop() {}
}
