package org.corfudb.common.protocol.client;

import io.netty.channel.EventLoopGroup;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Priority;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;

import java.net.InetSocketAddress;
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

    private Header getHeader(MessageType type) {
        return API.newHeader(generateRequestId(), priority, type, epoch, clusterId);
    }

    public CompletableFuture<Void> ping() {
        Header header = getHeader(MessageType.PING);
        return sendRequest(API.newPingRequest(header));
    }

    protected void handlePing(Response response) {
        completeRequest(response.getHeader().getRequestId(), null);
    }

    protected void handleRestart(Response response) {

    }

    protected void handleAuthenticate(Response response) {

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
