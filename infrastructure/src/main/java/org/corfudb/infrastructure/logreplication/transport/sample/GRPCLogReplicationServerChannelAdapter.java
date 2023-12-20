package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
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
    private Server server;

    private final int port;

    /*
     * GRPC Service Stub
     */
    private final GRPCLogReplicationServerHandler service;

    public GRPCLogReplicationServerChannelAdapter(ServerContext serverContext,
                                                  LogReplicationClientServerRouter router) {
        super(router);
        this.service = new GRPCLogReplicationServerHandler(router);
        this.port = Integer.parseInt((String) serverContext.getServerConfig().get("<port>"));
        // The executor of GRPCLogReplicationServerHandler needs to be single-threaded, otherwise the ordering of
        // requests and their acks cannot be guaranteed. By default, grpc utilizes thread-pool, so we need to provide
        // a single-threaded executor here.
        this.server = createGRPCServerInstance();
    }

    /**
     * If a gRPC server has started or shutdown, the same instance cannot be used for starting a gRPC server again,
     * and a new instance has to be created. This is a limitation introduced by gRPC.
     *
     * @return A newly instantiated gRPC server instance
     */
    private Server createGRPCServerInstance() {
        return ServerBuilder.forPort(port).addService(service)
                .executor(Executors.newSingleThreadScheduledExecutor()).build();
    }

    @Override
    public void send(CorfuMessage.ResponseMsg msg) {
        log.info("Server send response message {}", msg.getPayload().getPayloadCase());
        service.send(msg);
    }

    @Override
    public CompletableFuture<Boolean> start() throws IllegalStateException {
        CompletableFuture<Boolean> serverCompletable = new CompletableFuture<>();
        try {
            server.start();
            serverCompletable.complete(true);
            log.info("Server started, listening on {}", port);
        } catch (IllegalStateException ise) {
            log.warn("gRPC server already started or shutdown, instantiating a new one.", ise);
            this.server = createGRPCServerInstance();
            throw ise;
        } catch (Exception e) {
            log.error("Caught exception while starting server on port {}", port, e);
            serverCompletable.completeExceptionally(e);
        }

        return serverCompletable;
    }

    @Override
    public void stop() {
        log.info("Stop GRPC service.");
        if (server != null && !server.isShutdown() ) {
            server.shutdownNow();
        }
    }

}
