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
package org.corfudb.infrastructure;

import lombok.Getter;
import org.corfudb.infrastructure.configmaster.policies.IReconfigurationPolicy;
import org.corfudb.infrastructure.configmaster.policies.SimpleReconfigurationPolicy;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.entries.OldBundleEntry;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.CorfuDBViewSegment;
import org.corfudb.runtime.view.WriteOnceAddressSpace;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.configmasters.IConfigMaster;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.Headers;

import org.slf4j.MarkerFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.net.URLConnection;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.*;

import javax.json.JsonWriter;
import javax.json.Json;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonNumber;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;

import org.corfudb.runtime.UnwrittenException;
import org.corfudb.runtime.TrimmedException;

import java.util.UUID;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.IllegalAccessException;
import java.lang.IllegalArgumentException;

/*
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Connection;
*/
import org.corfudb.runtime.gossip.StreamEpochGossipEntry;
import org.corfudb.runtime.gossip.StreamDiscoveryRequestGossip;
import org.corfudb.runtime.gossip.StreamDiscoveryResponseGossip;
import org.corfudb.runtime.gossip.IGossip;

import org.corfudb.runtime.stream.Timestamp;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.runtime.view.RemoteLogView;
import org.corfudb.runtime.RemoteException;
import org.corfudb.runtime.view.StreamData;

import org.corfudb.runtime.gossip.StreamPullGossip;
import org.corfudb.runtime.gossip.StreamBundleGossip;
import org.corfudb.runtime.view.StreamingSequencer;
import org.corfudb.runtime.entries.CorfuDBStreamMoveEntry;
import org.corfudb.runtime.view.CachedWriteOnceAddressSpace;
import org.corfudb.runtime.view.Serializer;

import java.util.concurrent.CompletableFuture;

public class ConfigMasterServer implements ICorfuDBServer {

    private Logger log = LoggerFactory.getLogger(ConfigMasterServer.class);
    private Map<String,Object> config;
    private CorfuDBView currentView;
    private StreamView currentStreamView;
    private Boolean viewActive;
    private Boolean running;
    @Getter
    private Thread thread;
    private HttpServer server;

  //  private GossipServer gossipServer;
    private RemoteLogView currentRemoteView;
    int masterid = new SecureRandom().nextInt();

    public ConfigMasterServer() {
    }

    /*
    private class GossipServer {
      //  private Server server;
        private int port;

        public GossipServer(final Map<String,Object> config)
        {
            server = new Server(16384, 8192);
            port = (Integer) config.get("port");
            port += 1;
            IGossip.registerSerializer(server.getKryo());
        }

        public void start()
        {
         /  server.start();
            log.info("Gossip server bound to TCP/UDP port " + port);
            try {
                server.bind(port, port);
            }
            catch (IOException ie)
            {
                log.debug("Error binding gossip server", ie);
            }
           server.addListener(new Listener(){
                public void received (Connection connection, Object object)
                {
                    if (object instanceof StreamEpochGossipEntry)
                    {
                        StreamEpochGossipEntry sege = (StreamEpochGossipEntry) object;
                        if (!currentStreamView.checkStream(sege.streamID, sege.logPos))
                        {
                            if (!currentStreamView.learnStream(sege.streamID, sege.logID, null, -1, sege.epoch, sege.logPos))
                            {
                                log.debug("Learned about an epoch change for stream " + sege.streamID + ", but we don't know about that stream, discovering...");
                                StreamDiscoveryRequestGossip sdrg = new StreamDiscoveryRequestGossip(sege.streamID);
                                sendGossipToAllRemotes(sdrg);
                                return;
                            }
                            if (!sege.fromMaster)
                            {
                                /* now we need to advertise this change to all other configuration masters */
                             //   sege.fromMaster = true;
                //                sendGossipToAllRemotes(sege);
                          //  }
                     //   }
                   // }
    /*
                    else if (object instanceof StreamDiscoveryRequestGossip)
                    {
                        StreamDiscoveryRequestGossip sdrg = (StreamDiscoveryRequestGossip) object;
                        StreamData sd = currentStreamView.getStream(sdrg.streamID);
                        if (sd != null)
                        {
                            StreamDiscoveryResponseGossip sdresp = new StreamDiscoveryResponseGossip(
                                    sd.streamID,
                                    sd.currentLog,
                                    sd.startLog,
                                    sd.startPos,
                                    sd.epoch,
                                    sd.lastUpdate
                                    );
                            sendGossipToAllRemotes(sdresp);
                        }
                    }
                    else if (object instanceof StreamDiscoveryResponseGossip)
                    {
                        StreamDiscoveryResponseGossip sdrg = (StreamDiscoveryResponseGossip) object;
                        if (!currentStreamView.checkStream(sdrg.streamID, sdrg.logPos))
                        {
                            currentStreamView.learnStream(sdrg.streamID, sdrg.currentLog, sdrg.startLog, sdrg.startPos, sdrg.epoch, sdrg.logPos);
                        }
                    }
                    else if (object instanceof StreamPullGossip)
                    {
                        StreamPullGossip spg = (StreamPullGossip) object;
                        StreamingSequencer slocal = new StreamingSequencer(currentView);
                        long remoteToken = slocal.getNext(spg.streamID, spg.reservation);
                        WriteOnceAddressSpace woaslocal = new WriteOnceAddressSpace(currentView);
                        long streamEpoch = currentStreamView.getStream(spg.streamID).epoch;
                        try{
                        woaslocal.write(remoteToken, new CorfuDBStreamMoveEntry(spg.streamID, spg.destinationLog, spg.destinationStream, spg.physicalPos, spg.duration, streamEpoch, spg.destinationEpoch, spg.payload));
                        } catch (Exception e) {}
                    }
                    else if (object instanceof StreamBundleGossip)
                    {
                        log.debug("Executing bundle!");
                        StreamBundleGossip spg = (StreamBundleGossip) object;
                        StreamingSequencer slocal = new StreamingSequencer(currentView);
                        long remoteToken = slocal.getNext(spg.streamID, spg.reservation);
                        WriteOnceAddressSpace woaslocal = new WriteOnceAddressSpace(currentView);
                        long streamEpoch = currentStreamView.getStream(spg.streamID).epoch;
                        try{
                        woaslocal.write(remoteToken, new OldBundleEntry(spg.epochMap, spg.destinationLog, spg.destinationStream, spg.physicalPos,  streamEpoch, spg.payload, (int) spg.duration, spg.offset ));
                        } catch (Exception e) {}
                    }

                }
            });
        }

    }
*/

    private void sendGossipToAllRemotes(IGossip gossip)
    {
        for (UUID remote : currentRemoteView.getAllLogs())
        {
            try {
                log.info("send remote gossip to {}", remote);
                CorfuDBView cv = (CorfuDBView)currentRemoteView.getLog(remote);
                IConfigMaster cm = (IConfigMaster) cv.getConfigMasters().get(0);
                cm.sendGossip(gossip);
            } catch (Exception e)
            {
                log.debug("Error Broadcasting Gossip", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public ICorfuDBServer getInstance(final Map<String,Object> config)
    {
        //use the config for the init view (well, we'll have to deal with reconfigurations...)
       // gossipServer = new GossipServer(config);
     //  gossipServer.start();

        this.config = config;
        viewActive = false;
        currentView = new CorfuDBView(config);
        currentStreamView = new StreamView();
        currentRemoteView = new RemoteLogView();
        //UUID is going to be random for now, since this configuration is not persistent
        UUID logID =  UUID.randomUUID();
        log.info("New log instance id= " + logID.toString());
        currentView.setUUID(logID);
        thread = new Thread(this);
        return this;
    }

    @Override
    public void close() {
        if (server != null)
        {
            running = false;
            server.stop(0);
            this.getThread().interrupt();
        }
    }

    public void loadRemoteLogs()
    {
        if (config.get("remotelogs") != null)
        {
            for (String configMaster  : (List<String>) config.get("remotelogs"))
            {
                try {
                    UUID remoteID = currentRemoteView.addLog(configMaster, currentView.getUUID());
                    CorfuDBView view = currentRemoteView.getLog(remoteID);
                    if (view != null)
                    {
                        IConfigMaster cm = (IConfigMaster) view.getConfigMasters().get(0);
                        Map<UUID, String> remoteLogList = cm.getAllLogs();
                        for (UUID rlog : remoteLogList.keySet())
                        {
                           if (!rlog.equals(currentView.getUUID()))
                           {
                               if (currentRemoteView.addLog(rlog, remoteLogList.get(rlog)))
                               {
                                   log.info("Discovered new remote stream " + rlog);
                               }
                           }
                        }
                        //Tell the remote stream that we exist
                        cm.addLog(currentView.getUUID(), currentView.getConfigMasters().get(0).getFullString());
                    }
                }
                catch (Exception e)
                {
                    log.warn("Error talking to remote stream" ,e);
                }
            }
        }
    }

    public void checkViewThread() {
        log.info("Starting view check thread");
        while(running)
        {
            try {
                boolean success = currentView.isViewAccessible();
                if (success && !viewActive)
                {
                    log.info("New view is now accessible and active");
                    currentView.setEpoch(0);
                    viewActive = true;
                    synchronized(viewActive)
                    {
                        viewActive.notify();
                    }
                }
                else if(!success)
                {
                    log.info("View is not accessible, checking again in 30s");
                }
                //also check if all remote logs are still accessible.
                //and remove any which are not.
                currentRemoteView.checkAllLogs();
                loadRemoteLogs();

                synchronized(viewActive)
                {
                    viewActive.wait(30000);
                }
            }
            catch (InterruptedException ie)
            {

            }
        }
    }

    public void run()
    {
        try {
            running = true;
            log.info("Starting HTTP Service on port " + config.get("port"));
            server = HttpServer.create(new InetSocketAddress((Integer)config.get("port")), 0);
            server.createContext("/corfu", new RequestHandler());
            server.createContext("/control", new ControlRequestHandler());
            server.createContext("/", new StaticRequestHandler());
            server.setExecutor(null);
            server.start();
            loadRemoteLogs();
            checkViewThread();
        } catch(IOException ie) {
            log.error(MarkerFactory.getMarker("FATAL"), "Couldn't start HTTP Service!", ie);
        }
    }

    private void reset() {
        log.info("RESET requested, resetting all nodes and incrementing epoch");
        long newEpoch = currentView.getEpoch() + 1;

        for (IServerProtocol sequencer : currentView.getSequencers())
        {
            try {
            sequencer.reset(newEpoch);
            } catch (Exception e)
            {
                log.error("Error resetting sequencer", e);
            }
        }

        for (CorfuDBViewSegment vs : currentView.getSegments())
        {
            for (List<IServerProtocol> group : vs.getGroups())
            {
                for (IServerProtocol logunit: group)
                {
                    try {
                    logunit.reset(newEpoch);
                    }
                    catch (Exception e)
                    {
                        log.error("Error resetting logunit", e);
                    }
                }
            }
        }

        currentStreamView = new StreamView();
        //UUID is going to be random for now, since this configuration is not persistent
        UUID logID =  UUID.randomUUID();
        currentView.setUUID(logID);
        log.info("New log instance id= " + logID.toString());
        currentView.resetEpoch(newEpoch);
    }

    private JsonValue addStream(JsonObject params)
    {
        try {
            JsonObject jo = params;
            StreamData sd = currentStreamView.getStream(UUID.fromString(jo.getJsonString("streamid").getString()));
            boolean didnotalreadyexist = false;
            if (sd == null) {didnotalreadyexist = true;}
            currentStreamView.addStream(UUID.fromString(jo.getJsonString("streamid").getString()), UUID.fromString(jo.getJsonString("logid").getString()), jo.getJsonNumber("startpos").longValue());
            sd = currentStreamView.getStream(UUID.fromString(jo.getJsonString("streamid").getString()));

            if (didnotalreadyexist)
            {
            log.info("Adding new stream {}", sd.streamID);
            if (!(Boolean) jo.getBoolean("nopass", false))
            {

            for (UUID remote : currentRemoteView.getAllLogs())
            {
                CompletableFuture.runAsync(() -> {
                try {
                    log.info("send addstream to {}", remote);
                    CorfuDBView cv = (CorfuDBView)currentRemoteView.getLog(remote);
                    IConfigMaster cm = (IConfigMaster) cv.getConfigMasters().get(0);
                    cm.addStreamCM(UUID.fromString(jo.getJsonString("logid").getString()), UUID.fromString(jo.getJsonString("streamid").getString()), jo.getJsonNumber("startpos").longValue(), true);
                } catch (Exception e)
                {
                    log.debug("Error Broadcasting Gossip", e);
                }});
            }
            }
/*
            StreamDiscoveryResponseGossip sdresp = new StreamDiscoveryResponseGossip(
                    sd.streamID,
                    sd.currentLog,
                    sd.startLog,
                    sd.startPos,
                    sd.epoch,
                    sd.lastUpdate
                    );
            sendGossipToAllRemotes(sdresp);*/
            }
        }
        catch (Exception ex)
        {
            log.error("Error adding stream", ex);
            return JsonValue.FALSE;
        }
        return JsonValue.TRUE;
    }

    private JsonObject getStream(JsonObject params)
    {
        JsonObjectBuilder ob = Json.createObjectBuilder();

        try {
            JsonObject jo = params;
            log.debug(jo.toString());
            StreamData sd = currentStreamView.getStream(UUID.fromString(jo.getJsonString("streamid").getString()));
            if (sd == null)
            {
                ob.add("present", false);
            }
            else
            {
                ob.add("present", true);
                ob.add("currentlog", sd.currentLog.toString());
                ob.add("startlog", sd.startLog.toString());
                ob.add("startpos", sd.startPos);
                ob.add("epoch", sd.epoch);
            }

        }
        catch (Exception ex)
        {
            log.error("Error getting stream", ex);
            ob.add("present", false);
        }
        return ob.build();
    }

    private JsonObject getAllLogs(JsonObject params)
    {
        JsonObjectBuilder jb = Json.createObjectBuilder();
        Map<UUID, String> logs = currentRemoteView.getAllLogsMappings();
        for (UUID key : logs.keySet())
        {
            jb.add(key.toString(), logs.get(key));
        }
        return jb.build();
    }

    private JsonArray streamInfo(JsonObject params)
    {
        JsonArrayBuilder jb = Json.createArrayBuilder();
        Set<UUID> streams = currentStreamView.getAllStreams();
        try {
        for (UUID key : streams)
        {
            StreamData sd = currentStreamView.getStream(key);
            JsonObjectBuilder job = Json.createObjectBuilder();
            job.add("streamid", sd.streamID.toString());
            job.add("currentlog", sd.currentLog.toString());
            job.add("startlog", sd.startLog.toString());
            job.add("startpos", sd.startPos);
            job.add("epoch", sd.epoch);
            jb.add(job);
        }
        }
        catch (Exception ex)
        {
            log.debug("Exception getting streaminfo", ex);
        }
        return jb.build();
    }

    private String getLog(JsonObject params)
    {
        try {
            return currentRemoteView.getLogString(UUID.fromString(params.getString("logid")));
        }
        catch (RemoteException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    private JsonValue addLog(JsonObject params)
    {
       if (currentRemoteView.addLog(UUID.fromString(params.getString("logid")), params.getString("path")))
       {
           log.info("Learned new remote stream " + params.getString("logid"));
       }

        return JsonValue.FALSE;
    }

    public static byte[] toPrimitive(Byte[] array) {
        if (array == null) {
        return null;
        } else if (array.length == 0) {
        return null;
        }
        final byte[] result = new byte[array.length];
        for (int i = 0; i < array.length; i++) {
        result[i] = array[i];
        }
        return result;
    }

    private JsonObject streamInspector(JsonObject params)
    {
        long pos = params.getJsonNumber("pos").longValue();
        JsonObjectBuilder output = Json.createObjectBuilder();
        WriteOnceAddressSpace woas = new WriteOnceAddressSpace(currentView);
        return null;
    }

    private JsonObject reconfig(JsonObject params)
    {
        try {
            log.warn("Reconfiguration requested!");

            NetworkException ne = null;
            if (params.getJsonString("exception") != null) {
                String ex = params.getJsonString("exception").getString();
                ne = (NetworkException) Serializer.deserialize_compressed(Base64.getDecoder().decode(ex));
                log.warn("Reconfigure due to " +ne.getMessage());
            }
            IReconfigurationPolicy policy = new SimpleReconfigurationPolicy();
            currentView = policy.getNewView(currentView, ne);
            log.warn("Reconfiguration completed, view is now " + currentView.getSerializedJSONView().toString());
        }
        catch (Exception e)
        {
            log.warn("Exception" , e);
        }
        return null;
    }

    private JsonObject newView(JsonObject params)
    {
        try {
            log.warn("Forcibly installing new view");
            log.warn("newview= " + params.getJsonString("newview").getString());
            try (StringReader isr  = new StringReader(params.getJsonString("newview").getString()))
            {
                try (BufferedReader br = new BufferedReader(isr))
                {
                    try (JsonReader jr = Json.createReader(br))
                    {
                        JsonObject jo  = jr.readObject();
                        currentView = new CorfuDBView(jo);;
                    }
                    catch (Exception e)
                    {
                        log.error("error", e);
                    }
                }
            }
        } catch (Exception e)
        {
            log.warn("exception", e);
        }
        return null;
    }

    private JsonObject logInfo(JsonObject params)
    {
        long pos = params.getJsonNumber("pos").longValue();
        JsonObjectBuilder output = Json.createObjectBuilder();
        CachedWriteOnceAddressSpace woas = new CachedWriteOnceAddressSpace(currentView);
        try {
            output.add("state", "data");
            byte[] data = woas.read(pos);
            output.add("size", data.length);
            Object obj = Serializer.deserialize_compressed(data);
                    output.add("classname", obj.getClass().getName());
                    JsonObjectBuilder datan = Json.createObjectBuilder();
                    Class<?> current = obj.getClass();
                    do
                    {
                        Field[] fields = current.getDeclaredFields();
                        for (Field field: fields)
                        {
                            try{
                                if (!Modifier.isTransient(field.getModifiers()))
                                {
                                    if (field.getName().toString().equals("ts"))
                                    {
                                        Timestamp ts = (Timestamp) field.get(obj);
                                        if (ts!=null)
                                        {
                                            ts.physicalPos = pos;
                                        }
                                    }
                                    else if (field.getName().toString().equals("payload") && field.get(obj) != null)
                                    {
                                        try {
                                        JsonObjectBuilder pdatan = Json.createObjectBuilder();
                                        Object pobj = field.get(obj);
                                                pdatan.add("classname", pobj.getClass().getName());
                                                Class<?> pcurrent = pobj.getClass();
                                                try {
                                                pdatan.add("string", pobj.toString());
                                                } catch (Exception ex) {}
                                                do
                                                {
                                                    Field[] pfields = pcurrent.getDeclaredFields();
                                                    for (Field pfield : pfields)
                                                    {
                                                        try
                                                        {
                                                            Object podata = pfield.get(obj);
                                                            pdatan.add(pfield.getName().toString(), podata == null ? "null" :
                                                                                                    podata.toString() == null ? "null" :
                                                                                                    podata.toString());
                                                        } catch (IllegalArgumentException iae) {}
                                                        catch (IllegalAccessException iae) {}
                                                    }
                                                } while ((pcurrent = pcurrent.getSuperclass()) != null && pcurrent != Object.class);
                                        output.add("payload", pdatan);
                                        } catch (Exception e)
                                        {
                                            log.debug("Exception reading payload",e);
                                        }
                                    }
                                    Object odata = field.get(obj);
                                    datan.add(field.getName().toString(), odata == null ? "null" :
                                                                          odata.toString() == null ? "null" :
                                                                          odata.toString());
                                }
                            } catch (IllegalArgumentException iae) {}
                            catch (IllegalAccessException iae) {}
                        }
                    } while ((current = current.getSuperclass()) != null && current != Object.class);
                    output.add("data", datan);
        }
        catch (TrimmedException te)
        {
            output.add("state", "trimmed");
        }
        catch (UnwrittenException ue)
        {
            output.add("state", "unwritten");
        }
        catch (IOException ie)
        {
            output.add("classname", "unknown");
            output.add("error", ie.getMessage());

        }
        catch (ClassNotFoundException cnfe)
        {
            output.add("classname", "unknown");
            output.add("error", cnfe.getMessage());
        }

        return output.build();
    }

    private class StaticRequestHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            URI request = t.getRequestURI();
            String ignoreQuery = request.toString().split("\\?")[0];
            log.debug("Serving {}.", ignoreQuery);
            if (ignoreQuery.toString().equals("/"))
            {
                Headers h = t.getResponseHeaders();
                h.set("Location", "index.html");
                t.sendResponseHeaders(301, 0);
                return;
            }
            InputStream resourceStream = getClass().getResourceAsStream(ignoreQuery);
            if(resourceStream != null)
            {
                Headers h = t.getResponseHeaders();
                OutputStream os = t.getResponseBody();
                try {
                byte[] data = new byte[16384];
                int nread;
                long total = 0;
                String contentType = URLConnection.guessContentTypeFromStream(resourceStream);
                if (contentType == null) {
                    String extension = "";

                    int i = ignoreQuery.lastIndexOf('.');
                    if (i > 0) {
                        extension = request.toString().substring(i+1);
                    }
                    switch (extension)
                    {
                        case "js":
                            contentType = "application/javascript";
                            break;
                        case "woff":
                            contentType = "application/font-woff";
                            break;
                        case "eot":
                            contentType = "application/vnd.ms-fontobject";
                            break;
                        case "html":
                        case "htm":
                            contentType = "text/html";
                            break;
                        case "css":
                            contentType = "text/css";
                            break;
                        default:
                            contentType = "application/octet-stream";
                            break;
                    }
                }
                log.debug("Set content type to {}",  contentType);
                h.set("Content-Type", contentType);
                t.sendResponseHeaders(200, resourceStream.available());
                while ((nread = resourceStream.read(data, 0, data.length))!= -1)
                {
                    os.write(data, 0, nread);
                }
                }
                catch (Exception e)
                {
                    log.debug("error", e);
                }
                os.close();
            }
            else
            {
                String response = "File not found!";
                OutputStream os = t.getResponseBody();
                t.sendResponseHeaders(404, response.length());
                os.write(response.getBytes());
                os.close();
            }
        }
    }
    private class ControlRequestHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {

            try {
                String response = null;

                if (t.getRequestMethod().startsWith("POST")) {
                    log.debug("POST request:" + t.getRequestURI());
                    String apiCall = null;
                    JsonObject params = null;
                    JsonNumber id = null;
                    try (InputStreamReader isr = new InputStreamReader(t.getRequestBody(), "utf-8")) {
                        try (BufferedReader br = new BufferedReader(isr)) {
                            try (JsonReader jr = Json.createReader(br)) {
                                JsonObject jo = jr.readObject();
                                log.debug("request is " + jo.toString());
                                apiCall = jo.getString("method");
                                params = jo.getJsonObject("params");
                                id = jo.getJsonNumber("id");
                            } catch (Exception e) {
                                log.error("error", e);
                            }
                        }
                    }
                    Object result = JsonValue.FALSE;
                    JsonObjectBuilder job = Json.createObjectBuilder();
                    switch (apiCall) {
                        case "ping":
                            job.add("result", "pong");
                            break;
                        case "reset":
                            reset();
                            job.add("result", JsonValue.TRUE);
                            break;
                        case "addstream":
                            job.add("result", addStream(params));
                            break;
                        case "getstream":
                            job.add("result", getStream(params));
                            break;
                        case "getalllogs":
                            job.add("result", getAllLogs(params));
                            break;
                        case "addlog":
                            job.add("result", addLog(params));
                            break;
                        case "getlog":
                            job.add("result", getLog(params));
                            break;
                        case "loginfo":
                            job.add("result", logInfo(params));
                            break;
                        case "streaminfo":
                            job.add("result", streamInfo(params));
                            break;
                        case "streaminspector":
                            job.add("result", streamInspector(params));
                            break;
                        case "newview":
                            job.add("result", newView(params));
                            break;
                        case "reconfig":
                            reconfig(params);
                            break;
                    }
                    JsonObject res = job.add("calledmethod", apiCall)
                            .add("jsonrpc", "2.0")
                            .add("id", id)
                            .build();
                    response = res.toString();
                    log.info("Response is " + response);
                } else {
                    log.debug("PUT request");
                    response = "deny";
                }
                Headers h = t.getResponseHeaders();
                h.set("Content-Type", "application/json");
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();

            } catch (Exception e) {
                log.warn("Exception handling control request", e);
            }
        }
    }

    private class RequestHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {

            String response = null;

            if (t.getRequestMethod().startsWith("GET")) {
                log.debug("GET request:" + t.getRequestURI());
                StringWriter sw = new StringWriter();
                try (JsonWriter jw = Json.createWriter(sw))
                {
                    jw.writeObject(currentView.getSerializedJSONView());
                }
                response = sw.toString();
                log.debug("Response is", response);
            } else {
                log.debug("PUT request");
                response = "deny";
            }
                t.getResponseHeaders().add("content-type", "application/json");
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();

        }
    }

}

