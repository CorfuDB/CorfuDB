package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.LoadBalancerRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

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

    private final int port;

    /*
     * GRPC Service Stub
     */
    private final GRPCLogReplicationServerHandler service;

    private CompletableFuture<Boolean> serverCompletable;

    public GRPCLogReplicationServerChannelAdapter(ServerContext serverContext, LogReplicationServerRouter router) {
        super(serverContext, router);
        this.service = new GRPCLogReplicationServerHandler(router);
        this.port = Integer.parseInt((String) serverContext.getServerConfig().get("<port>"));
        // The executor of GRPCLogReplicationServerHandler needs to be single-threaded, otherwise the ordering of
        // requests and their acks cannot be guaranteed. By default, grpc utilizes thread-pool, so we need to provide
        // a single-threaded executor here.
        this.server = ServerBuilder.forPort(port).addService(service)
                .executor(Executors.newSingleThreadScheduledExecutor()).build();

        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
    }

    @Override
    public void send(CorfuMessage.ResponseMsg msg) {
        log.info("Server send message {}", msg.getPayload().getPayloadCase());
        service.send(msg);
    }

    @Override
    public CompletableFuture<Boolean> start() {
        try {
            serverCompletable = new CompletableFuture<>();
            server.start();
            log.info("Server started, listening on {}", port);
        } catch (Exception e) {
            log.error("Caught exception while starting server on port {}", port, e);
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
