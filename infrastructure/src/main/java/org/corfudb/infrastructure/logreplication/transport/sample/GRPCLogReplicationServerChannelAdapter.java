package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceServerRouter;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.Map;
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

    private final int port;

    /*
     * GRPC Service Stub
     */
    private final GRPCLogReplicationSinkServerHandler service;

    private CompletableFuture<Boolean> serverCompletable;

    public GRPCLogReplicationServerChannelAdapter(ServerContext serverContext,
                                                  Map<ReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer,
                                                  Map<ReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServer) {
        super(serverContext, sessionToSourceServer, sessionToSinkServer);
        this.service = new GRPCLogReplicationSinkServerHandler(sessionToSourceServer, sessionToSinkServer);
        this.port = Integer.parseInt((String) serverContext.getServerConfig().get("<port>"));
        this.server = ServerBuilder.forPort(port).addService(service).build();
    }

    @Override
    public void send(CorfuMessage.ResponseMsg msg) {
        log.info("Server send message {}", msg.getPayload().getPayloadCase());
        service.send(msg);
    }

    @Override
    public void send(String nodeId, CorfuMessage.RequestMsg msg) {
        log.info("Server send message {}", msg.getPayload().getPayloadCase());
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
