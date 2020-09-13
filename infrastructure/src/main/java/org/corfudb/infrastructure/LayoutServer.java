package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandlerContext;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.API;
import org.corfudb.infrastructure.ServerThreadFactory.ExceptionHandler;
import org.corfudb.infrastructure.paxos.PaxosDataStore;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.protocols.wireprotocol.LayoutCommittedRequest;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutPrepareRequest;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.protocols.wireprotocol.LayoutProposeRequest;
import org.corfudb.protocols.wireprotocol.LayoutProposeResponse;
import org.corfudb.runtime.protocol.proto.CorfuProtocol;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.BootstrapLayoutResponse;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.CommitLayoutResponse;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Header;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.PrepareLayoutResponse;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.ProposeLayoutResponse;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Request;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Response;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.ServerError;
import org.corfudb.runtime.view.Layout;

import java.util.Optional;

/**
 * The layout server serves layouts, which are used by clients to find the
 * Corfu infrastructure.
 *
 * <p>For replication and high availability, the layout server implements a
 * basic Paxos protocol. The layout server functions as a Paxos acceptor,
 * and accepts proposals from clients consisting of a rank and desired
 * layout. The protocol consists of three rounds:
 *
 * <p>1)   Prepare(rank) - Clients first contact each server with a rank.
 * If the server responds with ACK, the server promises not to
 * accept any requests with a rank lower than the given rank.
 * If the server responds with LAYOUT_PREPARE_REJECT, the server
 * informs the client of the current high rank and the request is
 * rejected.
 *
 * <p>2)   Propose(rank,layout) - Clients then contact each server with
 * the previously prepared rank and the desired layout. If no other
 * client has sent a prepare with a higher rank, the layout is
 * persisted, and the server begins serving that layout to other
 * clients. If the server responds with LAYOUT_PROPOSE_REJECT,
 * either another client has sent a prepare with a higher rank,
 * or this was a propose of a previously accepted rank.
 *
 * <p>3)   Committed(rank, layout) - Clients then send a hint to each layout
 * server that a new rank has been accepted by a quorum of
 * servers.
 *
 * <p>Created by mwei on 12/8/15.
 */
//TODO Finer grained synchronization needed for this class.
//TODO Need a janitor to cleanup old phases data and to fill up holes in layout history.
@Slf4j
public class LayoutServer extends AbstractServer {

    @Getter
    private final ServerContext serverContext;

    /**
     * HandlerMethod for this server.
     * [RM] Remove this after Protobuf for RPC Completion
     */
    @Getter
    private final HandlerMethods handler =
            HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    /**
     * RequestHandlerMethods for the Layout server
     */
    @Getter
    private final RequestHandlerMethods handlerMethods =
            RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);

    @NonNull
    private final ExecutorService executor;

    @NonNull
    private final PaxosDataStore paxosDataStore;

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        //[RM] Remove this after Protobuf for RPC Completion
        return getState() == ServerState.READY;
    }

    @Override
    public boolean isServerReadyToHandleReq(Header requestHeader) {
        return getState() == ServerState.READY;
    }

    /**
     * Returns new LayoutServer for context.
     *
     * @param serverContext context object providing settings and objects
     */
    public LayoutServer(@Nonnull ServerContext serverContext) {
        this.serverContext = serverContext;

        this.paxosDataStore = PaxosDataStore.builder()
                .dataStore(serverContext.getDataStore())
                .build();

        executor = Executors.newFixedThreadPool(
                serverContext.getLayoutServerThreadCount(),
                new ServerThreadFactory("layoutServer-", new ExceptionHandler())
        );

        if (serverContext.installSingleNodeLayoutIfAbsent()) {
            setLayoutInHistory(getCurrentLayout());
        }
    }

    @Override
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        //[RM] Remove this after Protobuf for RPC Completion
        executor.submit(() -> getHandler().handle(msg, ctx, r));
    }

    @Override
    protected void processRequest(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        executor.submit(() -> getHandlerMethods().handle(req, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

    private boolean isBootstrapped(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        //[RM] Remove this after Protobuf for RPC Completion
        if (getCurrentLayout() == null) {
            log.warn("Received message but not bootstrapped! Message={}", msg);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.LAYOUT_NOBOOTSTRAP));
            return false;
        }
        return true;
    }

    private boolean isBootstrapped(Header reqHeader, ChannelHandlerContext ctx, IRequestRouter r) {
        if(getCurrentLayout() == null) {
            r.sendNoBootstrapError(reqHeader, ctx);
            return false;
        }

        return true;
    }

    private void handleWrongEpochError(Header reqHeader, ChannelHandlerContext ctx, IRequestRouter r, long serverEpoch) {
        final Header responseHeader = API.generateResponseHeader(reqHeader, false, true);
        final ServerError wrongEpochError = API.getWrongEpochServerError(serverEpoch);

        final Response response = API.getErrorResponseNoPayload(responseHeader, wrongEpochError);
        r.sendResponse(response, ctx);
    }

    // Helper Methods

    /**
     * Handle a layout request message.
     *
     * @param msg              corfu message containing LAYOUT_REQUEST
     * @param ctx              netty ChannelHandlerContext
     * @param r                server router
     */
    @ServerHandler(type = CorfuMsgType.LAYOUT_REQUEST)
    public synchronized void handleMessageLayoutRequest(CorfuPayloadMsg<Long> msg,
                                                    ChannelHandlerContext ctx, IServerRouter r) {
        //[RM] Remove this after Protobuf for RPC Completion
        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        final long msgEpoch = msg.getPayload();
        final long serverEpoch = getServerEpoch();

        if (msgEpoch <= serverEpoch) {
            r.sendResponse(ctx, msg, new LayoutMsg(getCurrentLayout(), CorfuMsgType
                    .LAYOUT_RESPONSE));
        } else {
            // else the client is somehow ahead of the server.
            //TODO figure out a strategy to deal with this situation
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            log.warn("handleMessageLayoutRequest: Message Epoch {} ahead of Server epoch {}",
                    msgEpoch, serverEpoch);
        }
    }

    @RequestHandler(type = CorfuProtocol.MessageType.GET_LAYOUT)
    public synchronized void handleGetLayout(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        if(!isBootstrapped(req.getHeader(), ctx, r)) return;

        final long payloadEpoch = req.getGetLayoutRequest().getEpoch();
        final long serverEpoch = getServerEpoch();

        if(payloadEpoch <= serverEpoch) {
            Header responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
            Response response = API.getGetLayoutResponse(responseHeader, getCurrentLayout());
            r.sendResponse(response, ctx);
        } else {
            //TODO: Client is ahead of the server. Is any other handling required?
            log.warn("handleGetLayout[{}]: Payload epoch {} ahead of Server epoch {}",
                    req.getHeader().getRequestId(), payloadEpoch, serverEpoch);

            handleWrongEpochError(req.getHeader(), ctx, r, serverEpoch);
        }
    }

    /**
     * Sets the new layout if the server has not been bootstrapped with one already.
     *
     * @param msg corfu message containing LAYOUT_BOOTSTRAP
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.LAYOUT_BOOTSTRAP)
    public synchronized void handleMessageLayoutBootstrap(
            @NonNull CorfuPayloadMsg<LayoutBootstrapRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {
        //[RM] Remove this after Protobuf for RPC Completion

        if (getCurrentLayout() == null) {
            Layout layout = msg.getPayload().getLayout();

            log.info("handleMessageLayoutBootstrap: Bootstrap with new layout={}, {}",
                    layout, msg);

            if (layout.getClusterId() == null) {
                log.warn("handleMessageLayoutBootstrap: The layout does {} not have a clusterId",
                        layout);
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            }
            else{
                setCurrentLayout(layout);
                serverContext.setServerEpoch(layout.getEpoch(), r);
                //send a response that the bootstrap was successful.
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
            }
        } else {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("handleMessageLayoutBootstrap: Got a request to bootstrap a server which is "
                    + "already bootstrapped, rejecting!");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP));
        }
    }

    @RequestHandler(type = CorfuProtocol.MessageType.BOOTSTRAP_LAYOUT)
    public synchronized void handleBootstrapLayout(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        final Header requestHeader = req.getHeader();
        Header responseHeader;
        Response response;

        if(getCurrentLayout() == null) {
            final Layout layout = API.fromProtobufLayout(req.getBootstrapLayoutRequest().getLayout());
            log.info("handleBootstrapLayout[{}]: Bootstrap with new layout={}", requestHeader.getRequestId(), layout);

            if(layout.getClusterId() == null) {
                log.warn("handleBootstrapLayout[{}]: The layout={} does not have a clusterId",
                        requestHeader.getRequestId(), layout);

                responseHeader = API.generateResponseHeader(requestHeader, false, false);
                response = API.getBootstrapLayoutResponse(responseHeader, BootstrapLayoutResponse.Type.NACK);
            } else {
                setCurrentLayout(layout);
                serverContext.setServerEpoch(layout.getEpoch(), r);
                responseHeader = API.generateResponseHeader(requestHeader, false, true);
                response = API.getBootstrapLayoutResponse(responseHeader, BootstrapLayoutResponse.Type.ACK);
            }
        } else {
            log.warn("handleBootstrapLayout[{}]: Got a request to bootstrap a server which is "
                    + "already bootstrapped, rejecting!", requestHeader.getRequestId());

            responseHeader = API.generateResponseHeader(requestHeader, false, true);
            final ServerError alreadyBootstrapped = API.getBootstrappedServerError("BOOTSTRAPPED " +
                    "error triggered by " + requestHeader.toString());

            response = API.getErrorResponseNoPayload(responseHeader, alreadyBootstrapped);
        }

        r.sendResponse(response, ctx);
    }

    /**
     * Accepts a prepare message if the rank is higher than any accepted so far.
     *
     * @param msg corfu message containing LAYOUT_PREPARE
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    // TODO this can work under a separate lock for this step as it does not change the global
    // components
    @ServerHandler(type = CorfuMsgType.LAYOUT_PREPARE)
    public synchronized void handleMessageLayoutPrepare(
            @NonNull CorfuPayloadMsg<LayoutPrepareRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {
        //[RM] Remove this after Protobuf for RPC Completion

        // Check if the prepare is for the correct epoch
        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        final long payloadEpoch = msg.getPayload().getEpoch();
        final long serverEpoch = getServerEpoch();

        final Rank prepareRank = new Rank(msg.getPayload().getRank(), msg.getClientID());
        final Rank phase1Rank = getPhase1Rank(payloadEpoch);

        if (payloadEpoch != serverEpoch) {
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            log.trace("handleMessageLayoutPrepare: Incoming message with wrong epoch, got {}, "
                            + "expected {}, message was: {}",
                    msg.getPayload().getEpoch(), serverEpoch, msg);
            return;
        }

        Layout proposedLayout = getProposedLayout(payloadEpoch);

        // This is a prepare. If the rank is less than or equal to the phase 1 rank, reject.
        if (phase1Rank != null && prepareRank.lessThanEqualTo(phase1Rank)) {
            log.debug("handleMessageLayoutPrepare: Rejected phase 1 prepare of rank={}, "
                    + "phase1Rank={}", prepareRank, phase1Rank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PREPARE_REJECT.payloadMsg(new
                    LayoutPrepareResponse(phase1Rank.getRank(), proposedLayout)));
        } else {
            // Return the layout with the highest rank proposed before.
            Rank highestProposedRank = proposedLayout == null ? new Rank(-1L, msg.getClientID())
                    : getPhase2Rank(payloadEpoch);
            setPhase1Rank(prepareRank, payloadEpoch);
            log.debug("handleMessageLayoutPrepare: New phase 1 rank={}", prepareRank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PREPARE_ACK.payloadMsg(new
                    LayoutPrepareResponse(highestProposedRank.getRank(), proposedLayout)));
        }
    }

    @RequestHandler(type = CorfuProtocol.MessageType.PREPARE_LAYOUT)
    public synchronized void handlePrepareLayout(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        final Header requestHeader = req.getHeader();
        if(!isBootstrapped(requestHeader, ctx, r)) return;

        final long payloadEpoch = req.getPrepareLayoutRequest().getEpoch();
        final long serverEpoch = getServerEpoch();

        final Rank phase1Rank = getPhase1Rank(payloadEpoch);
        final Rank prepareRank = new Rank(req.getPrepareLayoutRequest().getRank(), API.getJavaUUID(requestHeader.getClientId()));

        if(payloadEpoch != serverEpoch) {
            handleWrongEpochError(requestHeader, ctx, r, serverEpoch);
            return;
        }

        final Layout proposedLayout = getProposedLayout(payloadEpoch);
        Header responseHeader;
        Response response;

        // If the PREPARE_LAYOUT rank is less than or equal to the highest phase 1 rank, reject.
        if(phase1Rank != null && prepareRank.lessThanEqualTo(phase1Rank)) {
            log.debug("handlePrepareLayout[{}]: Rejected phase 1 prepare of rank={}, phase1Rank={}",
                    requestHeader.getRequestId(), prepareRank, phase1Rank);

            responseHeader = API.generateResponseHeader(requestHeader, false, false);
            response = API.getPrepareLayoutResponse(responseHeader,
                    PrepareLayoutResponse.Type.REJECT, phase1Rank.getRank(), proposedLayout);
        } else {
            // Return the layout with the highest rank proposed before.
            Rank highestProposedRank = proposedLayout == null ?
                    new Rank(-1L, API.getJavaUUID(requestHeader.getClientId())) : getPhase2Rank(payloadEpoch);

            setPhase1Rank(prepareRank, payloadEpoch);
            log.debug("handlePrepareLayout[{}]: New phase 1 rank={}", requestHeader.getRequestId(), prepareRank);

            responseHeader = API.generateResponseHeader(requestHeader, false, true);
            response = API.getPrepareLayoutResponse(responseHeader,
                    PrepareLayoutResponse.Type.ACK, highestProposedRank.getRank(), proposedLayout);
        }

        r.sendResponse(response, ctx);
    }

    /**
     * Accepts a proposal for which it had accepted in the prepare phase.
     * A minor optimization is to reject any duplicate propose messages.
     *
     * @param msg corfu message containing LAYOUT_PROPOSE
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.LAYOUT_PROPOSE)
    public synchronized void handleMessageLayoutPropose(
            @NonNull CorfuPayloadMsg<LayoutProposeRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {
        //[RM] Remove this after Protobuf for RPC Completion

        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        final long payloadEpoch = msg.getPayload().getEpoch();
        final long serverEpoch = getServerEpoch();

        final Rank proposeRank = new Rank(msg.getPayload().getRank(), msg.getClientID());
        final Rank phase1Rank = getPhase1Rank(payloadEpoch);

        // Check if the propose is for the correct epoch
        if (payloadEpoch != serverEpoch) {
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            log.trace("handleMessageLayoutPropose: Incoming message with wrong epoch, got {}, "
                            + "expected {}, message was: {}", payloadEpoch, serverEpoch, msg);
            return;
        }
        // This is a propose. If no prepare, reject.
        if (phase1Rank == null) {
            log.debug("handleMessageLayoutPropose: Rejected phase 2 propose of rank={}, "
                    + "phase1Rank=none", proposeRank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PROPOSE_REJECT.payloadMsg(new
                    LayoutProposeResponse(-1)));
            return;
        }
        // This is a propose. If the rank in the proposal is less than or equal to the highest yet
        // observed prepare rank, reject.
        if (!proposeRank.equals(phase1Rank)) {
            log.debug("handleMessageLayoutPropose: Rejected phase 2 propose of rank={}, "
                    + "phase1Rank={}", proposeRank, phase1Rank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PROPOSE_REJECT.payloadMsg(new
                    LayoutProposeResponse(phase1Rank.getRank())));
            return;
        }

        final Rank phase2Rank = getPhase2Rank(payloadEpoch);
        final Layout proposeLayout = msg.getPayload().getLayout();

        // Make sure that the layout epoch is the same as the LayoutProposeRequest epoch.
        if (proposeLayout.getEpoch() != payloadEpoch) {
            log.debug("Phase II error: layout {} and payload {} epoch should be the same",
                    proposeLayout.getEpoch(), payloadEpoch);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PROPOSE_REJECT.payloadMsg(new
                    LayoutProposeResponse(phase1Rank.getRank())));
            return;
        }

        // In addition, if the rank in the propose message is equal to the current phase 2 rank
        // (already accepted message), reject.
        // This can happen in case of duplicate messages.
        if (proposeRank.equals(phase2Rank)) {
            log.debug("handleMessageLayoutPropose: Rejected phase 2 propose of rank={}, "
                    + "phase2Rank={}", proposeRank, phase2Rank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PROPOSE_REJECT.payloadMsg(new
                    LayoutProposeResponse(phase2Rank.getRank())));
            return;
        }

        log.debug("handleMessageLayoutPropose: New phase 2 rank={}, layout={}",
                proposeRank, proposeLayout);
        setPhase2Data(new Phase2Data(proposeRank, proposeLayout), payloadEpoch);
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
    }

    @RequestHandler(type = CorfuProtocol.MessageType.PROPOSE_LAYOUT)
    public synchronized void handleProposeLayout(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        final Header requestHeader = req.getHeader();
        if(!isBootstrapped(requestHeader, ctx, r)) return;

        final long payloadEpoch = req.getProposeLayoutRequest().getEpoch();
        final long serverEpoch = getServerEpoch();

        final Rank phase1Rank = getPhase1Rank(payloadEpoch);
        final Rank proposeRank = new Rank(req.getProposeLayoutRequest().getRank(), API.getJavaUUID(requestHeader.getClientId()));

        if(payloadEpoch != serverEpoch) {
            handleWrongEpochError(requestHeader, ctx, r, serverEpoch);
            return;
        }

        Header responseHeader;
        Response response;

        // If there is not corresponding PREPARE_LAYOUT, reject.
        if(phase1Rank == null) {
            log.debug("handleProposeLayout[{}]: Rejected phase 2 propose of rank={}, phase1Rank=none",
                    requestHeader.getRequestId(), proposeRank);

            responseHeader = API.generateResponseHeader(requestHeader, false, false);
            response = API.getProposeLayoutResponse(responseHeader, ProposeLayoutResponse.Type.REJECT, -1L);
            r.sendResponse(response, ctx);
            return;
        }

        // If the rank in PROPOSE_LAYOUT is less than or equal to the highest observed
        // rank from PREPARE_LAYOUT, reject.
        if(!proposeRank.equals(phase1Rank)) {
            log.debug("handleProposeLayout[{}]: Rejected phase 2 propose of rank={}, phase1Rank={}",
                    requestHeader.getRequestId(), proposeRank, phase1Rank);

            responseHeader = API.generateResponseHeader(requestHeader, false, false);
            response = API.getProposeLayoutResponse(responseHeader, ProposeLayoutResponse.Type.REJECT, phase1Rank.getRank());
            r.sendResponse(response, ctx);
            return;
        }

        final Rank phase2Rank = getPhase2Rank(payloadEpoch);
        final Layout proposeLayout = API.fromProtobufLayout(req.getProposeLayoutRequest().getLayout());

        // Make sure that the layout epoch is the same as the PROPOSE_LAYOUT epoch.
        if(proposeLayout.getEpoch() != payloadEpoch) {
            log.debug("handleProposeLayout[{}]: layout {} and payload {} epoch should be the same",
                    requestHeader.getRequestId(), proposeLayout.getEpoch(), payloadEpoch);

            responseHeader = API.generateResponseHeader(requestHeader, false, false);
            response = API.getProposeLayoutResponse(responseHeader, ProposeLayoutResponse.Type.REJECT, phase1Rank.getRank());
            r.sendResponse(response, ctx);
            return;
        }

        // In addition, if the rank in PROPOSE_LAYOUT is equal to the current phase 2 rank,
        // reject. This can happen in case of duplicate messages.
        if (proposeRank.equals(phase2Rank)) {
            log.debug("handleProposeLayout[{}]: Rejected phase 2 propose of rank={}, phase2Rank={}",
                    requestHeader.getRequestId(), proposeRank, phase2Rank);

            responseHeader = API.generateResponseHeader(requestHeader, false, false);
            response = API.getProposeLayoutResponse(responseHeader, ProposeLayoutResponse.Type.REJECT, phase2Rank.getRank());
            r.sendResponse(response, ctx);
            return;
        }

        log.debug("handleProposeLayout[{}]: New phase 2 rank={}, layout={}",
                requestHeader.getRequestId(), proposeRank, proposeLayout);

        setPhase2Data(new Phase2Data(proposeRank, proposeLayout), payloadEpoch);
        responseHeader = API.generateResponseHeader(requestHeader, false, true);
        response = API.getProposeLayoutResponse(responseHeader, ProposeLayoutResponse.Type.ACK, proposeRank.getRank());
        r.sendResponse(response, ctx);
    }

    /**
     * Force layout enables the server to bypass consensus
     * and accept a new layout.
     *
     * @param msg              corfu message containing LAYOUT_FORCE
     * @param ctx              netty ChannelHandlerContext
     * @param r                server router
     */
    private synchronized void forceLayout(@Nonnull CorfuPayloadMsg<LayoutCommittedRequest> msg,
                                               @Nonnull ChannelHandlerContext ctx,
                                               @Nonnull IServerRouter r) {
        //[RM] Remove this after Protobuf for RPC Completion
        final long payloadEpoch = msg.getPayload().getEpoch();
        final long serverEpoch = getServerEpoch();

        if (payloadEpoch != serverEpoch) {
            // return can't force old epochs
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            log.warn("forceLayout: Trying to force a layout with an old epoch. Layout {}, " +
                    "current epoch {}", payloadEpoch, serverEpoch);
            return;
        }

        final Layout msgLayout = msg.getPayload().getLayout();

        setCurrentLayout(msgLayout);
        serverContext.setServerEpoch(msgLayout.getEpoch(), r);
        log.warn("forceLayout: Forcing new layout {}", msgLayout);
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
    }

    private synchronized void forceLayout(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        final long payloadEpoch = req.getCommitLayoutRequest().getEpoch();
        final long serverEpoch = getServerEpoch();
        final Header requestHeader = req.getHeader();
        Header responseHeader;
        Response response;

        if(payloadEpoch != serverEpoch) {
            log.warn("forceLayout[{}]: Trying to force a layout with an old epoch: payloadEpoch={}, serverEpoch={}",
                    requestHeader.getRequestId(), payloadEpoch, serverEpoch);

            responseHeader = API.generateResponseHeader(requestHeader, false, false);
            response = API.getCommitLayoutResponse(responseHeader, CommitLayoutResponse.Type.NACK);
            r.sendResponse(response, ctx);
            return;
        }

        final Layout layout = API.fromProtobufLayout(req.getCommitLayoutRequest().getLayout());

        setCurrentLayout(layout);
        serverContext.setServerEpoch(layout.getEpoch(), r);
        log.warn("forceLayout[{}]: Forcing new layout={}", requestHeader.getRequestId(), layout);
        responseHeader = API.generateResponseHeader(requestHeader, false, true);
        response = API.getCommitLayoutResponse(responseHeader, CommitLayoutResponse.Type.ACK);
        r.sendResponse(response, ctx);
    }

    /**
     * Accepts any committed layouts for the current epoch or newer epochs.
     * As part of the accept, the server changes it's current layout and epoch.
     *
     * @param msg corfu message containing LAYOUT_COMMITTED
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    // TODO If a server does not get SEAL layout commit message cannot reach it
    // TODO as this message is not set to ignore EPOCH.
    // TODO How do we handle holes in history if we let in layout commit message. Maybe we have a
    // hole filling process
    @ServerHandler(type = CorfuMsgType.LAYOUT_COMMITTED)
    public synchronized void handleMessageLayoutCommit(
            @NonNull CorfuPayloadMsg<LayoutCommittedRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {
        //[RM] Remove this after Protobuf for RPC Completion

        if (msg.getPayload().getForce()) {
            forceLayout(msg, ctx, r);
            return;
        }

        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        final long payloadEpoch = msg.getPayload().getEpoch();
        final long serverEpoch = getServerEpoch();
        final Layout msgLayout = msg.getPayload().getLayout();

        if (payloadEpoch < serverEpoch) {
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            return;
        }

        setCurrentLayout(msgLayout);
        serverContext.setServerEpoch(payloadEpoch, r);
        log.info("New layout committed: {}", msgLayout);
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
    }

    @RequestHandler(type = CorfuProtocol.MessageType.COMMIT_LAYOUT)
    public synchronized void handleCommitLayout(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        if(req.getCommitLayoutRequest().getForced()) {
            forceLayout(req, ctx, r);
            return;
        }

        if(!isBootstrapped(req.getHeader(), ctx, r)) return;

        final long payloadEpoch = req.getCommitLayoutRequest().getEpoch();
        final long serverEpoch = getServerEpoch();
        final Layout layout = API.fromProtobufLayout(req.getCommitLayoutRequest().getLayout());

        if(payloadEpoch < serverEpoch) {
            handleWrongEpochError(req.getHeader(), ctx, r, serverEpoch);
            return;
        }

        setCurrentLayout(layout);
        serverContext.setServerEpoch(payloadEpoch, r);

        Header responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
        Response response = API.getCommitLayoutResponse(responseHeader, CommitLayoutResponse.Type.ACK);
        log.info("handleCommitLayout[{}]: New layout committed: {}", req.getHeader().getRequestId(), layout);
        r.sendResponse(response, ctx);
    }

    public Layout getCurrentLayout() {
        Layout layout = serverContext.getCurrentLayout();
        if (layout != null) {
            return new Layout(layout);
        } else {
            return null;
        }
    }

    /**
     * Sets the current layout in context DataStore.
     *
     * @param layout layout to set
     */
    public void setCurrentLayout(Layout layout) {
        serverContext.setCurrentLayout(layout);
        // set the layout in history as well
        setLayoutInHistory(layout);
    }

    public Rank getPhase1Rank(long epoch) {
        return paxosDataStore
                .getPhase1Rank(epoch)
                .orElse(null);
    }

    public void setPhase1Rank(Rank rank, long epoch) {
        paxosDataStore.setPhase1Rank(rank, epoch);
    }

    public void setPhase2Data(Phase2Data phase2Data, long epoch) {
        paxosDataStore.setPhase2Data(phase2Data, epoch);
    }

    public void setLayoutInHistory(Layout layout) {
        serverContext.setLayoutInHistory(layout);
    }

    private long getServerEpoch() {
        return serverContext.getServerEpoch();
    }

    /**
     * Returns the phase 2 rank.
     *
     * @return the phase 2 rank
     */
    public Rank getPhase2Rank(long epoch) {
        return paxosDataStore
                .getPhase2Data(epoch)
                .map(Phase2Data::getRank)
                .orElse(null);
    }

    /**
     * Returns the proposed layout received in phase 2 data.
     *
     * @return the proposed layout
     */
    public Layout getProposedLayout(long epoch) {
        return paxosDataStore
                .getPhase2Data(epoch)
                .map(Phase2Data::getLayout)
                .orElse(null);
    }

    /**
     * Return accepted data for the given epoch.
     *
     * @param epoch that we are interested in
     * @return {@link Optional} {@link Phase2Data}
     */
    @VisibleForTesting
    Optional<Phase2Data> getPhase2Data(long epoch) {
        return paxosDataStore.getPhase2Data(epoch);
    }
}
