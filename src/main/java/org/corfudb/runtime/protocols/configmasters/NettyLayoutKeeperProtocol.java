package org.corfudb.runtime.protocols.configmasters;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.wireprotocol.*;
import org.corfudb.runtime.protocols.AbstractNettyProtocol;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.NettyRPCChannelInboundHandlerAdapter;
import org.corfudb.runtime.view.CorfuDBView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonObject;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Created by dmalkhi on 11/6/15.
 */
public class NettyLayoutKeeperProtocol extends AbstractNettyProtocol<NettyLayoutKeeperProtocol.NettyLayoutServerHandler> implements ILayoutKeeper {
    private static final transient Logger log = LoggerFactory.getLogger(NettyLayoutKeeperProtocol.class);

    public static String getProtocolString() {
        return "cdbmk";
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        return new NettyLayoutKeeperProtocol(host, port, options, epoch);
    }

    public NettyLayoutKeeperProtocol(String host, Integer port, Map<String,String> options, long epoch)
    {
        super(host, port, options, epoch, new NettyLayoutServerHandler());
    }

    public CompletableFuture<JsonObject> getCurrentView() {
        log.info("try to get current view");
        NettyLayoutConfigMsg r = new NettyLayoutConfigMsg(
                NettyCorfuMsg.NettyCorfuMsgType.META_QUERY_REQ,
                getEpoch(),
                -1);
        CompletableFuture<JsonObject> ret = handler.sendMessageAndGetCompletable(getEpoch(), r);
        log.info("wait for current view via completableFuture");
        return ret;
    }

    public CompletableFuture<LayoutKeeperInfo> proposeNewView(int rank, JsonObject jo) {
        log.info("try to set new view");
        NettyLayoutConfigMsg r = new NettyLayoutConfigMsg(
                NettyCorfuMsg.NettyCorfuMsgType.META_PROPOSE_REQ,
                getEpoch(),
                rank, jo);
        CompletableFuture<LayoutKeeperInfo> ret = handler.sendMessageAndGetCompletable(getEpoch(), r);
        log.info("wait for propose ack via completableFuture");
        return ret;

    }

    static class NettyLayoutServerHandler extends NettyRPCChannelInboundHandlerAdapter {

        //region Handler Interface
        @Override
        public void handleMessage(NettyCorfuMsg message)
        {
            NettyLayoutConfigMsg m = (NettyLayoutConfigMsg) message;

            switch (message.getMsgType())
            {
                case PONG:
                    completeRequest(message.getRequestID(), true);
                    break;
                case META_COLLECT_RES:
                case META_PROPOSE_RES:
                    completeRequest(message.getRequestID(), new LayoutKeeperInfo(m.getEpoch(), m.getRank(), m.getJo()));
                    break;
                case META_QUERY_RES:
                    completeRequest(message.getRequestID(), m.getJo());
                    break;
            }
        }
        //endregion
    }

    /**
     * Gets the current view from the configuration master.
     *
     * @return The current view.
     */
    @Override
    public CorfuDBView getView() {

        return new CorfuDBView((JsonObject) getCurrentView().join());
    }

    @Override
    public void setBootstrapView(JsonObject initialView) {
        proposeNewView(-1, initialView).join(); // todo ...
    }
}
