package org.corfudb.runtime.clients;

import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.VersionInfo;

/**
 * This is a base client which sends basic messages.
 * It mainly sends PINGs, as well as the ACK/NACKs defined by
 * the Corfu protocol.
 * This is also responsible to send SEAL messages used to seal the servers with an epoch
 *
 * <p>Created by mwei on 12/9/15.
 */
@Slf4j
public class BaseClient implements IClient {

    /**
     * The router to use for the client.
     */
    @Getter
    @Setter
    private IClientRouter router;

    private final long epoch;

    public BaseClient(IClientRouter router, long epoch) {
        this.router = router;
        this.epoch = epoch;
    }

    /**
     * Ping the endpoint, synchronously.
     * Note: this ping is epoch aware
     *
     * @return True, if the endpoint was reachable, false otherwise.
     */
    public boolean pingSync() {
        try {
            return ping().get();
        } catch (Exception e) {
            log.error("Ping failed due to exception", e);
            return false;
        }
    }

    /**
     * Sets the epoch on client router and on the target layout server.
     *
     * @param newEpoch New Epoch to be set
     * @return Completable future which returns true on successful epoch set.
     */
    public CompletableFuture<Boolean> sealRemoteServer(long newEpoch) {
        CorfuMsg msg = new CorfuPayloadMsg<>(CorfuMsgType.SEAL, newEpoch).setEpoch(epoch);
        log.info("sealRemoteServer: send SEAL from me(clientId={}) to new epoch {}",
                msg.getClientID(), epoch);
        return router.sendMessageAndGetCompletable(msg);
    }

    public CompletableFuture<VersionInfo> getVersionInfo() {
        return router.sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.VERSION_REQUEST).setEpoch(epoch));
    }


    /**
     * Ping the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     * the endpoint is reachable, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> ping() {
        return router.sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.PING).setEpoch(epoch));
    }

    /**
     * Reset the endpoint, asynchronously.
     * WARNING: ALL EXISTING DATA ON THIS NODE WILL BE LOST.
     *
     * @return A completable future which will be completed with True if
     * the endpoint acks, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> reset() {
        return router.sendMessageAndGetCompletable(new CorfuMsg(CorfuMsgType.RESET)
                .setEpoch(epoch));
    }

    /**
     * Restart the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     * the endpoint acks, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> restart() {
        return router.sendMessageAndGetCompletable(new CorfuMsg(CorfuMsgType.RESTART)
                .setEpoch(epoch));
    }
}
