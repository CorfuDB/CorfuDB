package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.view.Layout;

import java.util.Map;
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
                    .add(CorfuMsgType.MANAGEMENT_BOOTSTRAP)
                    .add(CorfuMsgType.FAILURE_DETECTED)
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
            case MANAGEMENT_BOOTSTRAP:
                router.completeRequest(msg.getRequestID(), ((CorfuPayloadMsg<ManagementBootstrapRequest>) msg).getPayload());
                break;
            case FAILURE_DETECTED:
                router.completeRequest(msg.getRequestID(), ((CorfuPayloadMsg<FailureDetectorMsg>) msg).getPayload());
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
        return router.sendMessageAndGetCompletable(CorfuMsgType.MANAGEMENT_BOOTSTRAP.payloadMsg(new ManagementBootstrapRequest(l)));
    }

    /**
     * Sends the failure detected to the relevant management server.
     *
     * @param nodes The failed nodes map to be handled.
     * @return A future which will be return TRUE if completed successfully else returns FALSE.
     */
    public CompletableFuture<Boolean> handleFailure(Map nodes) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.FAILURE_DETECTED.payloadMsg(new FailureDetectorMsg(nodes)));
    }
}
