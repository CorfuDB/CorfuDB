package org.corfudb.infrastructure.logreplication.transport.server;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.infrastructure.logreplication.transport.IChannelContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSubscriber;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceServerRouter;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.Map;
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

    private final Map<ReplicationSession, LogReplicationSourceServerRouter> sesionToSourceServerRouter;
    private final Map<ReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServerRouter;

    @Getter
    private final ServerContext serverContext;

    /**
     * Constructs a new {@link IServerChannelAdapter}
     *
     * @param serverContext
     * @param sesionToSourceServerRouter map of session-> source-server router. Using this, the adapter forwards the
     *                                   msg to the correct source router.
     * @param sessionToSinkServerRouter map of session-> sink-server router. Using this, the adapter forwards the
     *                                  msg to the correct source router.
     */
    public IServerChannelAdapter(ServerContext serverContext,
                                 Map<ReplicationSession, LogReplicationSourceServerRouter> sesionToSourceServerRouter,
                                 Map<ReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServerRouter) {
        this.serverContext = serverContext;
        this.sesionToSourceServerRouter = sesionToSourceServerRouter;
        this.sessionToSinkServerRouter = sessionToSinkServerRouter;
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
     * @param nodeId remote node id
     * @param request corfu message to be sent
     */
    public abstract void send(String nodeId, CorfuMessage.RequestMsg request);

    /**
     * Receive a message from Server.
     * The adapter will forward this message to the router for further processing.
     *
     * @param msg received corfu message
     */
    public void receive(CorfuMessage.ResponseMsg msg) {
        ReplicationSession session = null;
        if (msg.getPayload().getPayloadCase().equals(CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_METADATA_RESPONSE)) {
            session = convertSessionMsg(msg.getPayload().getLrMetadataResponse().getSessionInfo());
        } else if (msg.getPayload().getPayloadCase().equals(CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_ENTRY_ACK)) {
            session = convertSessionMsg(msg.getPayload().getLrEntryAck().getMetadata().getSessionInfo());
        }
        if(sesionToSourceServerRouter.containsKey(session)) {
            sesionToSourceServerRouter.get(session).receive(msg);
        } else if(sessionToSinkServerRouter.containsKey(session)){
            sessionToSinkServerRouter.get(session).receive(msg);
        }
    }

    /**
     * Receive a message from Client.
     * @param msg received corfu message
     */
    public void receive(CorfuMessage.RequestMsg msg) {
        ReplicationSession session = null;
        if(msg.getPayload().getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_LEADERSHIP_QUERY)) {
            session = convertSessionMsg(msg.getPayload().getLrLeadershipQuery().getSessionInfo());
        } else if (msg.getPayload().getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_METADATA_REQUEST)) {
            session = convertSessionMsg(msg.getPayload().getLrMetadataRequest().getSessionInfo());
        } else if (msg.getPayload().getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_ENTRY)) {
            session = convertSessionMsg(msg.getPayload().getLrEntry().getMetadata().getSessionInfo());
        }
        if(sesionToSourceServerRouter.containsKey(session)) {
            sesionToSourceServerRouter.get(session).receive(msg);
        } else if(sessionToSinkServerRouter.containsKey(session)){
            sessionToSinkServerRouter.get(session).receive(msg);
        }
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


    private ReplicationSession convertSessionMsg(LogReplication.ReplicationSessionMsg sessionMsg) {
        log.info("sessionMsg : {}", sessionMsg);
        ReplicationSubscriber subscriber = new ReplicationSubscriber(sessionMsg.getReplicationModel(), sessionMsg.getClient());
        return new ReplicationSession(sessionMsg.getRemoteClusterId(), sessionMsg.getLocalClusterId(), subscriber);
    }
}
