package org.corfudb.common.protocol;

import io.netty.channel.EventLoopGroup;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;

import java.net.InetSocketAddress;

/**
 * Created by Maithem on 7/1/20.
 */

public class PeerClient extends ClientHandler {

    public PeerClient(InetSocketAddress remoteAddress, EventLoopGroup eventLoopGroup, long requestTimeoutInMs) {
        super(remoteAddress, eventLoopGroup, requestTimeoutInMs);

    }

    protected void handlePing(Response response) {

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
}
