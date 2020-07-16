package org.corfudb.common.protocol.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.ChannelImplementation;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Priority;
import org.corfudb.common.protocol.proto.CorfuProtocol.StreamAddressRange;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by Maithem on 7/1/20.
 */

@Slf4j
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
        return API.newHeader(generateRequestId(), priority, type, epoch,
               API.DEFAULT_UUID, config.getClientId(), ignoreClusterId, ignoreEpoch);
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
        // TODO(Zach): Handle timeout?
        return sendRequest(API.newAuthenticateRequest(header, config.getClientId(), config.getNodeId()));
    }

    protected void handleAuthenticate(Response response) {
        UUID serverId = new UUID(response.getAuthenticateResponse().getServerId().getMsb(),
                                response.getAuthenticateResponse().getServerId().getLsb());
        String corfuVersion = response.getAuthenticateResponse().getCorfuVersion();

        // Validate handshake, but first verify if node identifier is set to default (all 0's)
        // which indicates node id matching is not required.
        if(config.getNodeId().equals(API.DEFAULT_UUID)) {
            log.info("handleAuthenticate: node id matching is not requested by client.");
        } else if(!config.getNodeId().equals(serverId)) {
            log.error("handleAuthenticate: Handshake validation failed. Server node id mismatch.");
            log.debug("handleAuthenticate: Client opened socket to server [{}] instead, connected to: [{}]",
                    config.getNodeId(), serverId);
            // TODO(Zach): Any remaining handling
            return;
        }

        log.info("handleAuthenticate: Handshake succeeded. Server Corfu Version: [{}]", corfuVersion);
        // TODO(Zach): Signal success
        // completeRequest(response.getHeader().getRequestId(), response.getAuthenticateResponse());
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

    public static void main(String[] args) throws Exception{
        ClientConfig config = new ClientConfig(
                100000,
                100000,
                100000,
                100000,
                "",
                "",
                "",
                "",
                "",
                "",
                ChannelImplementation.NIO,
                false,
                false,
                100000,
                false,
                false,
                new UUID(1234,1234),
                API.DEFAULT_UUID
        );
        InetSocketAddress remoteAddress = new InetSocketAddress(InetAddress.getByName("localhost"),9000);
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        PeerClient peerClient = new PeerClient(remoteAddress, ChannelImplementation.NIO.getGenerator().generate(10,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("peer-client-%d")
                        .build()),
                config);
        log.info(peerClient.ping().get().toString());
    }
}
