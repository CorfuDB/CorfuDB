package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.FailureDetectorMsg;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.view.Layout;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A client to the Management Server.
 * <p>
 * Failure Detection:
 * This client allows a client to trigger failures handlers with relevant failures.
 * <p>
 * Created by zlokhandwala on 11/4/16.
 */
public class ManagementClient implements IClient {

    /**
     * The messages this client should handle.
     */
    @Getter
    public final Set<CorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<CorfuMsgType>()
                    .add(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST)
                    .add(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR)
                    .add(CorfuMsgType.MANAGEMENT_ALREADY_BOOTSTRAP_ERROR)
                    .add(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED)
                    .add(CorfuMsgType.MANAGEMENT_START_FAILURE_HANDLER)
                    .add(CorfuMsgType.HEARTBEAT_REQUEST)
                    .add(CorfuMsgType.HEARTBEAT_RESPONSE)
                    .build();

    @Setter
    @Getter
    IClientRouter router;

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     */
    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        switch (msg.getMsgType()) {
            case MANAGEMENT_NOBOOTSTRAP_ERROR:
                router.completeExceptionally(msg.getRequestID(), new NoBootstrapException());
                break;
            case MANAGEMENT_ALREADY_BOOTSTRAP_ERROR:
                router.completeExceptionally(msg.getRequestID(), new AlreadyBootstrappedException());
                break;
            case HEARTBEAT_RESPONSE:
                router.completeRequest(msg.getRequestID(), ((CorfuPayloadMsg<byte[]>)msg).getPayload());
                break;
        }
    }

    /**
     * Bootstraps a management server.
     *
     * @param l The layout to bootstrap with.
     * @return A completable future which will return TRUE if the
     * bootstrap was successful, false otherwise.
     */
    public CompletableFuture<Boolean> bootstrapManagement(Layout l) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(l));
    }

    /**
     * Sends the failure detected to the relevant management server.
     *
     * @param nodes The failed nodes map to be handled.
     * @return A future which will be return TRUE if completed successfully else returns FALSE.
     */
    public CompletableFuture<Boolean> handleFailure(Set nodes) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED.payloadMsg(new FailureDetectorMsg(nodes)));
    }

    /**
     * Initiates failure handling in the Management Server.
     *
     * @return A future which returns TRUE if failure handler triggered successfully.
     */
    public CompletableFuture<Boolean> initiateFailureHandler() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.MANAGEMENT_START_FAILURE_HANDLER.msg());
    }

    /**
     * Requests for a heartbeat message containing the node status.
     *
     * @return A future which will return the node health metrics of
     * the node which was requested for the heartbeat.
     */
    public CompletableFuture<byte[]> sendHeartbeatRequest() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.HEARTBEAT_REQUEST.msg());
    }
}
