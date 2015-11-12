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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dmalkhi on 11/6/15.
 */
public class NettyLayoutServerProtocol extends AbstractNettyProtocol<NettyLayoutServerProtocol.NettyLayoutServerHandler> implements IConfigMaster {
    private static final transient Logger log = LoggerFactory.getLogger(NettyLayoutServerProtocol.class);

    private String host;
    private Integer port;
    private Map<String,String> options;
    private long epoch;
    private JSONRPC2Session jsonSession;
    private AtomicInteger id;
    //private Client client;

    public static String getProtocolString() {
        log.info("cdbls");
        return "cdbls";
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        return new NettyLayoutServerProtocol(host, port, options, epoch);
    }

    public NettyLayoutServerProtocol(String host, Integer port, Map<String,String> options, long epoch)
    {
        super(host, port, options, epoch, new NettyLayoutServerHandler());
        log.info("NettyLayoutServerProtocol constructed");
    }

    public CompletableFuture<JsonObject> getCurrentView() {
        log.info("try to get current view");
        NettyLayoutServerRequestMsg r = new NettyLayoutServerRequestMsg(Json.createObjectBuilder().build());
        CompletableFuture<JsonObject> ret = handler.sendMessageAndGetCompletable(getEpoch(), r);
        log.info("wait for current view via completableFuture");
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
                case LAYOUT_RES:
                    log.info("response arrived, completing future");
                    completeRequest(message.getRequestID(), ((NettyLayoutServerResponseMsg)message).getJo());
                    break;
            }
        }
        //endregion
    }

    public Integer getPort()
    {
        return port;
    }

    public String getHost()
    {
        return host;
    }

    public Map<String,String> getOptions()
    {
        return options;
    }

    public StreamData getStream(UUID streamID)
    {
        //probably should do this over UDP.
        try {
            JSONRPC2Request jr = new JSONRPC2Request("getstream", id.getAndIncrement());
            Map<String, Object> params = new HashMap<String,Object>();
            params.put("streamid", streamID.toString());
            jr.setNamedParams(params);
            JSONRPC2Response jres = jsonSession.send(jr);
            if (jres.indicatesSuccess())
            {
                Map<String, Object> jo = (Map<String,Object>) jres.getResult();
                if ((Boolean) jo.get("present"))
                {
                    StreamData sd = new StreamData();
                    sd.currentLog = UUID.fromString((String) jo.get("currentlog"));
                    sd.startPos = (Long) jo.get("startpos");
                    sd.epoch = (Long) jo.get("epoch");
                    sd.startLog = UUID.fromString((String) jo.get("startlog"));
                    return sd;
                }
                else
                {
                    return null;
                }
            }
        } catch(Exception e) {
            log.error("other error", e);
            return null;
        }
        return null;
    }

    public boolean addStream(UUID logID, UUID streamID, long pos)
    {
        int counter = 0;
        while (counter< 3)
        {
            try {
                JSONRPC2Request jr = new JSONRPC2Request("addstream", id.getAndIncrement());
                Map<String, Object> params = new HashMap<String,Object>();
                params.put("logid", logID.toString());
                params.put("streamid", streamID.toString());
                params.put("startpos", pos);
                params.put("nopass", false);
                jr.setNamedParams(params);
                JSONRPC2Response jres = jsonSession.send(jr);
                if (jres.indicatesSuccess() && (Boolean) jres.getResult())
                {
                    return true;
                }
                return false;
            } catch(Exception e) {
                log.debug("Error sending addstream", e);
            }
            counter++;
        }
        return false;
    }
    public boolean addStreamCM(UUID logID, UUID streamID, long pos, boolean nopass)
    {
        try {
            JSONRPC2Request jr = new JSONRPC2Request("addstream", id.getAndIncrement());
            Map<String, Object> params = new HashMap<String,Object>();
            params.put("logid", logID.toString());
            params.put("streamid", streamID.toString());
            params.put("startpos", pos);
            params.put("nopass", nopass);

            jr.setNamedParams(params);
            JSONRPC2Response jres = jsonSession.send(jr);
            if (jres.indicatesSuccess() && (Boolean) jres.getResult())
            {
                return true;
            }
            return false;
        } catch(Exception e) {
            log.debug("Error sending addstream", e);
            return false;
        }
    }

    public boolean addLog(UUID logID, String path)
    {
        try {
            JSONRPC2Request jr = new JSONRPC2Request("addlog", id.getAndIncrement());
            Map<String, Object> params = new HashMap<String,Object>();
            params.put("logid", logID.toString());
            params.put("path", path);
            jr.setNamedParams(params);
            JSONRPC2Response jres = jsonSession.send(jr);
            if (jres.indicatesSuccess() && (Boolean) jres.getResult())
            {
                return true;
            }
            return false;
        } catch(Exception e) {
            return false;
        }
    }

    public String getLog(UUID logID)
    {
        try {
            JSONRPC2Request jr = new JSONRPC2Request("getlog", id.getAndIncrement());
            Map<String, Object> params = new HashMap<String,Object>();
            params.put("logid", logID.toString());
            jr.setNamedParams(params);
            JSONRPC2Response jres = jsonSession.send(jr);
            if (jres.indicatesSuccess())
            {
                return (String)jres.getResult();
            }
            return null;
        } catch(Exception e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public Map<UUID, String> getAllLogs()
    {
        try {
            JSONRPC2Request jr = new JSONRPC2Request("getalllogs", id.getAndIncrement());
            JSONRPC2Response jres = jsonSession.send(jr);
            if (jres.indicatesSuccess())
            {
                Map<UUID, String> resultMap = new HashMap<UUID, String>();
                Map<String, String> iresultMap = (Map<String,String>) jres.getResult();
                for (String s : iresultMap.keySet())
                {
                    resultMap.put(UUID.fromString(s), iresultMap.get(s));
                }
                return resultMap;
            }
            return null;
        } catch(Exception e) {
            return null;
        }

    }

    public void resetAll()
    {
        try {
            JSONRPC2Request jr = new JSONRPC2Request("reset", id.getAndIncrement());
            JSONRPC2Response jres = jsonSession.send(jr);
            if (jres.indicatesSuccess() && (Boolean) jres.getResult())
            {
            }
        } catch(Exception e) {
        }

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

    /**
     * Request reconfiguration due to a network exception.
     *
     * @param e The network exception that caused the reconfiguration request.
     */
    @Override
    public void requestReconfiguration(NetworkException e) {
        try {
            JSONRPC2Request jr = new JSONRPC2Request("reconfig", id.getAndIncrement());
            Map<String, Object> params = new HashMap<String,Object>();
            if (e != null) {
                params.put("exception", java.util.Base64.getEncoder().encodeToString(Serializer.serialize_compressed(e)));
            }
            jr.setNamedParams(params);
            JSONRPC2Response jres = jsonSession.send(jr);
            if (jres.indicatesSuccess() && (Boolean) jres.getResult())
            {
            }
        } catch(Exception ex) {
        }
    }

    /**
     * Force the configuration master to install a new view. This method should only be called during
     * testing.
     *
     * @param v The new view to install.
     */
    @Override
    public void forceNewView(CorfuDBView v) {
        try {
            JSONRPC2Request jr = new JSONRPC2Request("newview", id.getAndIncrement());
            Map<String, Object> params = new HashMap<String,Object>();
            params.put("newview", v.getSerializedJSONView().toString());
            jr.setNamedParams(params);
            JSONRPC2Response jres = jsonSession.send(jr);
            if (jres.indicatesSuccess() && (Boolean) jres.getResult())
            {
            }
        } catch(Exception e) {
        }
    }

}
