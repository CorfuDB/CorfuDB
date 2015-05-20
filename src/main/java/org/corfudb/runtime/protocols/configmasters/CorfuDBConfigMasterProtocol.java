/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.runtime.protocols.configmasters;

import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;

import java.util.Map;
import java.util.HashMap;

import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import com.thetransactioncompany.jsonrpc2.client.*;
import com.thetransactioncompany.jsonrpc2.*;
import java.net.*;

import org.corfudb.runtime.gossip.IGossip;
import org.corfudb.runtime.view.StreamData;

import com.esotericsoftware.kryonet.Client;

public class CorfuDBConfigMasterProtocol implements IServerProtocol, IConfigMaster
{
    private String host;
    private Integer port;
    private Map<String,String> options;
    private long epoch;
    private static final transient Logger log = LoggerFactory.getLogger(CorfuDBConfigMasterProtocol.class);
    private JSONRPC2Session jsonSession;
    private AtomicInteger id;
    private Client client;


     public static String getProtocolString()
    {
        return "cdbcm";
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

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        return new CorfuDBConfigMasterProtocol(host, port, options, epoch);
    }

    public CorfuDBConfigMasterProtocol(String host, Integer port, Map<String,String> options, Long epoch)
    {
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;
        this.id = new AtomicInteger();


        try
        {
            jsonSession = new JSONRPC2Session(new URL("http://"+ host + ":" + port + "/control"));
            JSONRPC2SessionOptions opts = new JSONRPC2SessionOptions();
            opts.setReadTimeout(5000);
            jsonSession.setOptions(opts);
            client = new Client(8192,8192);
            IGossip.registerSerializer(client.getKryo());
            client.start();
            client.connect(5000, host, port+1, port+1);
        }
        catch (Exception ex)
        {
            log.warn("Failed to connect to endpoint " + getFullString(), ex);
            throw new RuntimeException("Failed to connect to endpoint");
        }

    }

    public boolean ping()
    {
        try
        {
            JSONRPC2Request jr = new JSONRPC2Request("ping", id.getAndIncrement());
            JSONRPC2Response jres = jsonSession.send(jr);
            if (jres.indicatesSuccess() && jres.getResult().equals("pong"))
            {
                return true;
            }
        }
        catch(Exception e)
        {
            return false;
        }

        return false;
    }

    public void reset(long epoch)
    {

    }

    public void setEpoch(long epoch)
    {

    }

    public void sendGossip(IGossip gossip)
    {
            client.sendTCP(gossip);
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
        return null;
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
