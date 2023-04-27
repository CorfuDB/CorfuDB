package org.corfudb.infrastructure.logreplication.transport.server;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
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
@Slf4j
public abstract class IServerChannelAdapter {

    @Getter
    private final LogReplicationClientServerRouter router;

    /**
     * Constructs a new {@link IServerChannelAdapter}
     *
     * @param router interface between LogReplication and the transport server
     */
    public IServerChannelAdapter(LogReplicationClientServerRouter router) {
        this.router = router;
    }

    /**
     * Send message across channel.
     *
     * @param msg corfu message (protoBuf definition)
     */
    public abstract void send(CorfuMessage.ResponseMsg msg);

    /**
     * Send a message across the channel to a specific endpoint.
     *
     * @param request corfu message to be sent
     */
    public abstract void send(CorfuMessage.RequestMsg request);

    /**
     * Receive a message from Server.
     * The adapter will forward this message to the router for further processing.
     *
     * @param msg received corfu message
     */
    public void receive(CorfuMessage.ResponseMsg msg) {
        router.receive(msg);
    }

    /**
     * Receive a message from Client.
     * @param msg received corfu message
     */
    public void receive(CorfuMessage.RequestMsg msg) {
        router.receive(msg);
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
