package org.corfudb.infrastructure.remotecorfutable;

import io.netty.channel.ChannelHandlerContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServerRouter;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableRequestMsg;

import javax.annotation.Nonnull;
import java.util.UUID;

@Slf4j
public class RemoteCorfuTableRequestHandler {
    private final DatabaseHandler databaseHandler;

    public RemoteCorfuTableRequestHandler(@NonNull DatabaseHandler databaseHandler) {
        this.databaseHandler = databaseHandler;
    }

    public void handle(@Nonnull RequestMsg req, @Nonnull ChannelHandlerContext ctx,
                       @Nonnull IServerRouter r) {
        RemoteCorfuTableRequestMsg msg = req.getPayload().getRemoteCorfuTableRequest();
        switch (msg.getPayloadCase()) {
            case GET:
                handleGet(req, ctx, r);
                break;
            case SCAN:
                handleScan(req, ctx, r);
                break;
            case CONTAINS_KEY:
                handleContainsKey(req, ctx, r);
                break;
            case CONTAINS_VALUE:
                handleContainsValue(req, ctx, r);
                break;
            case SIZE:
                handleSize(req, ctx, r);
                break;
            default:
                log.error("handle[{}]: Unknown orchestrator request type {}",
                        req.getHeader().getRequestId(), msg.getPayloadCase());
                break;
        }
    }

    private void handleSize(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        UUID streamID = getUUID(req.getPayload().getRemoteCorfuTableRequest().getSize().getStreamID());
        long timestamp = req.getPayload().getRemoteCorfuTableRequest().getSize().getTimestamp();
        int scanSize = req.getPayload().getRemoteCorfuTableRequest().getSize().getInternalScanSize();
        databaseHandler.sizeAsync(streamID, timestamp, scanSize).thenAccept(resultSize -> {
//            r.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()),
//                    ));
        });
    }

    private void handleContainsValue(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
    }

    private void handleContainsKey(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
    }

    private void handleScan(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
    }

    private void handleGet(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
    }
}
