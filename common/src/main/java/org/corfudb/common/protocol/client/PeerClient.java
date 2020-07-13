package org.corfudb.common.protocol.client;

import io.netty.channel.EventLoopGroup;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Priority;
import org.corfudb.common.protocol.proto.CorfuProtocol.StreamAddressRange;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by Maithem on 7/1/20.
 */

public class PeerClient extends ChannelHandler {

    // set epoch
    // client id
    // cluster id

    final Priority priority = Priority.NORMAL;

    volatile long epoch = -1;

    final UUID clusterId = null;

    public PeerClient(InetSocketAddress remoteAddress, EventLoopGroup eventLoopGroup, ClientConfig config) {
        super(remoteAddress, eventLoopGroup, config);
    }

    private Header getHeader(MessageType type, boolean ignoreClusterId, boolean ignoreEpoch) {
        /* TODO(Zach): Incorporate clientId into header? */
        return API.newHeader(generateRequestId(), priority, type,
                epoch, clusterId, ignoreClusterId, ignoreEpoch);
    }

    public CompletableFuture<Void> ping() {
        Header header = getHeader(MessageType.PING, true, true);
        return sendRequest(API.newPingRequest(header));
    }

    protected void handlePing(Response response) {
        completeRequest(response.getHeader().getRequestId(), null);
    }

    protected void handleRestart(Response response) {

    }

    public CompletableFuture<Response> authenticate() {
        Header header = getHeader(MessageType.AUTHENTICATE, false, true);
        // TODO(Zach): Where to get serverId in UUID form? When to use which?
        // TODO(Zach): Handle timeout?
        return sendRequest(API.newAuthenticateRequest(header, config.getClientId(), API.DEFAULT_UUID));
    }

    // TODO: Handled in ClientHandshakeHandler?
    protected void handleAuthenticate(Response response) {
        UUID serverId = new UUID(response.getAuthenticateResponse().getServerId().getMsb(),
                                response.getAuthenticateResponse().getServerId().getLsb());
        String corfuVersion = response.getAuthenticateResponse().getCorfuVersion();

        // if nodeId == API.DEFAULT_UUID or nodeId == serverId then handshake successful
        // else handshake failed
    }

    protected void handleSeal(Response response) {

    }

    protected void  handleGetLayout(Response response) {

    }

    protected void handlePrepareLayout(Response response) {

    }

    protected void handleProposeLayout(Response response) {

    }

    protected void handleCommitLayout(Response response) {

    }

    protected void handleGetToken(Response response) {

    }

    protected void handleCommitTransaction(Response response) {

    }

    protected void handleBootstrap(Response response) {

    }


    public CompletableFuture<Response> getStreamsAddressSpace(List<StreamAddressRange> streamsAddressesRange) {
        Header header = getHeader(MessageType.QUERY_STREAM, false, false);
        return sendRequest(API.newQueryStreamRequest(header, streamsAddressesRange));
    }

    protected void handleQueryStream(Response response) {

    }

    protected void handleReadLog(Response response) {

    }

    protected void handleQueryLogMetadata(Response response) {

    }

    protected void handleTrimLog(Response response) {

    }

    protected void handleCompactLog(Response response) {

    }

    protected void handleFlash(Response response) {

    }

    protected void handleQueryNode(Response response) {

    }

    protected void handleReportFailure(Response response) {

    }

    protected void handleHealFailure(Response response) {

    }

    protected void handleExecuteWorkFlow(Response response) {

    }

    public void close() {
        // TODO(Maithem): implement all these
        // stop accepting requests (error new requests)
        // error out existing requests
        // close channel
        // update stats
        // change client state
        // add a close lock
    }
}
