package org.corfudb.common.protocol.client;

import io.netty.channel.EventLoopGroup;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by Maithem on 7/1/20.
 */

@Slf4j
@NoArgsConstructor
public class PeerClient extends ChannelHandler {

    // set epoch
    // client id
    // cluster id

    final Priority priority = Priority.NORMAL;

    volatile long epoch = -1;

    final UUID clusterId = API.DEFAULT_UUID;

    public PeerClient(InetSocketAddress remoteAddress, EventLoopGroup eventLoopGroup, ClientConfig config) {
        super(remoteAddress, eventLoopGroup, config);
    }

    private Header getHeader(MessageType type, boolean ignoreClusterId, boolean ignoreEpoch) {
        return API.newHeader(generateRequestId(), priority, type, epoch,
                clusterId, config.getClientId(), ignoreClusterId, ignoreEpoch);
    }

    /**
     * Ping the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     * the endpoint is reachable, otherwise False or exceptional completion.
     */
    protected CompletableFuture<Boolean> ping() {
        Header header = getHeader(MessageType.PING, true, true);
        log.info("ping: send PING from me(clientId={}) to the server",
                header.getClientId());
        return sendRequest(API.newPingRequest(header));
    }

    protected void handlePing(Response response) {
        completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Restart the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     * the endpoint acks, otherwise False or exceptional completion.
     */
    protected CompletableFuture<Boolean> restart() {
        Header header = getHeader(MessageType.RESTART, true, true);
        log.warn("restart: send RESTART from me(clientId={}) to the server",
                header.getClientId());
        return sendRequest(API.newRestartRequest(header));
    }

    protected void handleRestart(Response response) {
        log.warn("handleRestart: Restart response received from the server with " +
                        "LSB: {} MSB:{}", response.getHeader().getClientId().getLsb(),
                response.getHeader().getClientId().getMsb());
        completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Reset the endpoint, asynchronously.
     * WARNING: ALL EXISTING DATA ON THIS NODE WILL BE LOST.
     *
     * @return A completable future which will be completed with True if
     * the endpoint acks, otherwise False or exceptional completion.
     */
    protected CompletableFuture<Boolean> reset() {
        Header header = getHeader(MessageType.RESET, true, true);
        log.warn("reset: send RESET from me(clientId={}) to the server",
                header.getClientId());
        return sendRequest(API.newResetRequest(header));
    }

    protected void handleReset(Response response) {
        log.warn("handleReset: Reset response received from the server with " +
                        "LSB: {} MSB:{}", response.getHeader().getClientId().getLsb(),
                response.getHeader().getClientId().getMsb());
        completeRequest(response.getHeader().getRequestId(), true);
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
        if (config.getNodeId().equals(API.DEFAULT_UUID)) {
            log.info("handleAuthenticate: node id matching is not requested by client.");
        } else if (!config.getNodeId().equals(serverId)) {
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

    /**
     * Sets the epoch on client router and on the target layout server.
     *
     * @param newEpoch New Epoch to be set
     * @return Completable future which returns true on successful epoch set.
     */
    public CompletableFuture<Boolean> sealRemoteServer(long newEpoch) {
        Header header = getHeader(MessageType.SEAL, false, true);
        log.info("sealRemoteServer: send SEAL from me(clientId={}) to new epoch {}",
                header.getClientId(), newEpoch);
        return sendRequest(API.newSealRequest(header, newEpoch));
    }

    protected void handleSeal(Response response) {
        log.warn("handleReset: SealResponse received from the server with " +
                        "LSB: {} MSB:{}", response.getHeader().getClientId().getLsb(),
                response.getHeader().getClientId().getMsb());
    }

    protected void handleGetLayout(Response response) {

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
