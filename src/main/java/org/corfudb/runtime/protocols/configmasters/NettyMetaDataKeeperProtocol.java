package org.corfudb.runtime.protocols.configmasters;

import com.thetransactioncompany.jsonrpc2.JSONRPC2Request;
import com.thetransactioncompany.jsonrpc2.JSONRPC2Response;
import com.thetransactioncompany.jsonrpc2.client.JSONRPC2Session;
import org.corfudb.infrastructure.wireprotocol.*;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.AbstractNettyProtocol;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.NettyRPCChannelInboundHandlerAdapter;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.Serializer;
import org.corfudb.runtime.view.StreamData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonObject;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Created by dmalkhi on 11/6/15.
 */
public class NettyMetaDataKeeperProtocol extends AbstractNettyProtocol<NettyMetaDataKeeperProtocol.NettyLayoutServerHandler> implements IMetaData {
    private static final transient Logger log = LoggerFactory.getLogger(NettyMetaDataKeeperProtocol.class);

    public static String getProtocolString() {
        return "cdbmk";
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        return new NettyMetaDataKeeperProtocol(host, port, options, epoch);
    }

    public NettyMetaDataKeeperProtocol(String host, Integer port, Map<String,String> options, long epoch)
    {
        super(host, port, options, epoch, new NettyLayoutServerHandler());
    }

    public CompletableFuture<JsonObject> getCurrentView() {
        log.info("try to get current view");
        NettyCollectRequestMsg r = new NettyCollectRequestMsg(0);
        CompletableFuture<JsonObject> ret = handler.sendMessageAndGetCompletable(getEpoch(), r);
        log.info("wait for current view via completableFuture");
        return ret;
    }

    public CompletableFuture<Boolean> proposeNewView(int rank, JsonObject jo) {
        log.info("try to set new view");
        NettyProposeRequestMsg r = new NettyProposeRequestMsg(rank, jo);
        CompletableFuture<Boolean> ret = handler.sendMessageAndGetCompletable(getEpoch(), r);
        log.info("wait for propose ack via completableFuture");
        return ret;

    }

    static class NettyLayoutServerHandler extends NettyRPCChannelInboundHandlerAdapter {

        //region Handler Interface
        @Override
        public void handleMessage(NettyCorfuMsg message)
        {
            switch (message.getMsgType())
            {
                case PONG:
                    completeRequest(message.getRequestID(), true);
                    break;
                case META_COLLECT_RES:
                    completeRequest(message.getRequestID(), ((NettyCollectResponseMsg)message).getJo());
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
        proposeNewView(-1, initialView); // .join();
    }
}
