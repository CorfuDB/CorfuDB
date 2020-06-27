package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;

import java.util.concurrent.CompletableFuture;

/**
 * Server GRPC Transport Adapter
 *
 * This router is a default implementation used for transport plugin tests.
 */
@Slf4j
public class GRPCLogReplicationServerChannelAdapter extends IServerChannelAdapter {

    /*
     * GRPC Server used for listening and dispatching incoming calls.
     */
    private final Server server;

    /*
     * GRPC Service Stub
     */
    private final GRPCLogReplicationServerHandler service;

    private CompletableFuture<Boolean> serverCompletable;

    public GRPCLogReplicationServerChannelAdapter(ServerContext serverContext, LogReplicationServerRouter router) {
        super(serverContext, router);
        this.service = new GRPCLogReplicationServerHandler(router);
        this.server = ServerBuilder.forPort(getPort()).addService(service).build();
    }

    @Override
    public void send(CorfuMessage msg) {
        log.info("Server send message {}", msg.getType());
        service.send(msg);
    }

    @Override
    public CompletableFuture<Boolean> start() {
        try {
            serverCompletable = new CompletableFuture<>();
            server.start();
            log.info("Server started, listening on " + this.getPort());
        } catch (Exception e) {
            log.error("Caught exception while starting server on port {}", getPort(), e);
            serverCompletable.completeExceptionally(e);
        }

        return serverCompletable;
    }

    @Override
    public void stop() {
        log.info("Stop GRPC service.");
        if (server != null) {
            server.shutdownNow();
        }
        if (serverCompletable != null) {
            serverCompletable.complete(true);
        }
    }

}
