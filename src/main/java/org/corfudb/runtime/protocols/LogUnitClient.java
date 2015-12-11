package org.corfudb.runtime.protocols;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.wireprotocol.*;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 12/10/15.
 */
public class LogUnitClient implements INettyClient {
    @Setter
    NettyClientRouter router;

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     */
    @Override
    public void handleMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx) {
        switch (msg.getMsgType())
        {
            case ERROR_OK:
                router.completeRequest(msg.getRequestID(), true);
                break;
            case ERROR_TRIMMED:
                router.completeExceptionally(msg.getRequestID(), new Exception("Trimmed"));
                break;
            case ERROR_OVERWRITE:
                router.completeExceptionally(msg.getRequestID(), new Exception("Overwrite"));
                break;
            case ERROR_OOS:
                router.completeExceptionally(msg.getRequestID(), new Exception("OOS"));
                break;
            case ERROR_RANK:
                router.completeExceptionally(msg.getRequestID(), new Exception("Rank"));
                break;
        }
    }

    /** The messages this client should handle. */
    @Getter
    public final Set<NettyCorfuMsg.NettyCorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<NettyCorfuMsg.NettyCorfuMsgType>()
                    .add(NettyCorfuMsg.NettyCorfuMsgType.WRITE)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.READ_REQUEST)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.READ_RESPONSE)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.TRIM)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.FILL_HOLE)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.FORCE_GC)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.GC_INTERVAL)

                    .add(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OK)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.ERROR_TRIMMED)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OVERWRITE)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OOS)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.ERROR_RANK)
                    .build();

    /**
     * Asynchronously write to the logging unit.
     *
     * @param address     The address to write to.
     * @param streams     The streams, if any, that this write belongs to.
     * @param rank        The rank of this write (used for quorum replication).
     * @param writeObject The object, pre-serialization, to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    public CompletableFuture<Boolean> write(long address, Set<UUID> streams, long rank, Object writeObject) {
        NettyLogUnitWriteMsg w = new NettyLogUnitWriteMsg(address);
        w.setStreams(streams);
        w.setRank(rank);
        w.setPayload(writeObject);
        return router.sendMessageAndGetCompletable(w);
    }
}
