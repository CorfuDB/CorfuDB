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

import org.corfudb.client.CorfuDBView;
import org.corfudb.client.CorfuDBViewSegment;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.IServerProtocol;
import org.corfudb.client.configmasters.IConfigMaster;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.Headers;
import org.apache.thrift.TException;

import org.slf4j.MarkerFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URLConnection;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.*;

import javax.json.JsonWriter;
import javax.json.Json;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonNumber;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;

import org.corfudb.client.UnwrittenException;
import org.corfudb.client.TrimmedException;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import java.util.UUID;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.IllegalAccessException;
import java.lang.IllegalArgumentException;

import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Connection;

import org.corfudb.client.gossip.StreamEpochGossipEntry;
import org.corfudb.client.gossip.IGossip;

import org.corfudb.client.Timestamp;
import org.corfudb.client.StreamView;

public class ConfigMasterServer implements Runnable, ICorfuDBServer {

    private Logger log = LoggerFactory.getLogger(ConfigMasterServer.class);
    private Map<String,Object> config;
    private CorfuDBView currentView;
    private StreamView currentStreamView;
    private Boolean viewActive;
    private GossipServer gossipServer;
    int masterid = new SecureRandom().nextInt();

    public ConfigMasterServer() {
    }

    private class GossipServer {
        private Server server;
        private int port;

        public GossipServer(final Map<String,Object> config)
        {
            server = new Server();
            port = (Integer) config.get("port");
            port += 1;
            IGossip.registerSerializer(server.getKryo());
        }

        public void start()
        {
            server.start();
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
                            log.debug("Got a new epoch gossip message at position " + sege.logPos);
                            currentStreamView.learnStream(sege.streamID, sege.logID, null, -1, sege.epoch, sege.logPos);
                        }
                    }
                }
            });
        }

    }

    @SuppressWarnings("unchecked")
    public Runnable getInstance(final Map<String,Object> config)
    {
        //use the config for the init view (well, we'll have to deal with reconfigurations...)
        gossipServer = new GossipServer(config);
        gossipServer.start();

        this.config = config;
        viewActive = false;
        currentView = new CorfuDBView(config);
        currentStreamView = new StreamView();
        //UUID is going to be random for now, since this configuration is not persistent
        UUID logID =  UUID.randomUUID();
        log.info("New log instance id= " + logID.toString());
        currentView.setUUID(logID);
        currentView.addRemoteLog(logID, currentView.getConfigMasters().get(0).getFullString());
        log.info("Remote log set");
        return this;
    }

    public void checkViewThread() {
        log.info("Starting view check thread");
        while(true)
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
                for (UUID remoteLog : currentView.getAllLogs().keySet())
                {
                    IConfigMaster cm = CorfuDBView.getConfigurationMasterFromString(currentView.getAllLogs().get(remoteLog));
                    if (!cm.ping())
                    {
                        log.debug("Remote log " + remoteLog + " not accessible, removing after 2 retries.");
                        Thread.sleep(5000);
                        if (!cm.ping())
                        {
                            Thread.sleep(5000);
                            if (!cm.ping())
                            {
                                log.debug("Removing inaccessible remote log {}", remoteLog);
                                currentView.getAllLogs().remove(remoteLog);
                            }
                        }
                    }
                }

                if (config.get("remotelogs") != null)
                {
                    for (Object configmaster  : (List<Object>) config.get("remotelogs"))
                    {
                        try {
                            IConfigMaster cm = CorfuDBView.getConfigurationMasterFromString((String) configmaster);
                            //Get the list of logs that the remote knows
                            Map<UUID, String> remoteLogList = null;
                            for (int i = 0; i < 5; i++)
                            {
                                remoteLogList = cm.getAllLogs();
                                if (remoteLogList != null) { break; }
                                else {log.debug("Remote log " + configmaster + " inaccessible, waiting 5 s and retrying"); Thread.sleep(5000);}
                            }
                            for (UUID rlog : remoteLogList.keySet())
                            {
                               if (currentView.addRemoteLog(rlog, remoteLogList.get(rlog)))
                               {
                                   log.info("Discovered new remote log " + rlog);
                               }
                            }
                            //Tell the remote log that we exist
                            cm.addLog(currentView.getUUID(), currentView.getConfigMasters().get(0).getFullString());
                        }
                        catch (Exception e)
                        {
                            log.warn("Error talking to remote log" ,e);
                        }
                    }
                }

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
            log.info("Starting HTTP Service on port " + config.get("port"));
            HttpServer server = HttpServer.create(new InetSocketAddress((Integer)config.get("port")), 0);
            server.createContext("/corfu", new RequestHandler());
            server.createContext("/control", new ControlRequestHandler());
            server.createContext("/", new StaticRequestHandler());
            server.setExecutor(null);
            server.start();
            checkViewThread();
        } catch(IOException ie) {
            log.error(MarkerFactory.getMarker("FATAL"), "Couldn't start HTTP Service!" , ie);
            System.exit(1);
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

        currentView.resetEpoch(newEpoch);
    }

    private JsonValue addStream(JsonObject params)
    {
        try {
            JsonObject jo = params;
            currentStreamView.addStream(UUID.fromString(jo.getJsonString("streamid").getString()), currentView.getUUID(), jo.getJsonNumber("startpos").longValue());
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
            StreamView.StreamData sd = currentStreamView.getStream(UUID.fromString(jo.getJsonString("streamid").getString()));
            if (sd == null)
            {
                ob.add("present", false);
            }
            else
            {
                ob.add("present", true);
                ob.add("startpos", sd.startPos);
                ob.add("logid", UUID.randomUUID().toString());
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
        Map<UUID, String> logs = currentView.getAllLogs();
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
            StreamView.StreamData sd = currentStreamView.getStream(key);
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
        return currentView.getRemoteLog(UUID.fromString(params.getString("logid")));
    }

    private JsonValue addLog(JsonObject params)
    {
       if (currentView.addRemoteLog(UUID.fromString(params.getString("logid")), params.getString("path")))
       {
           log.info("Learned new remote log " + params.getString("logid"));
       }

        return JsonValue.FALSE;
    }

    private JsonObject logInfo(JsonObject params)
    {
        long pos = params.getJsonNumber("pos").longValue();
        JsonObjectBuilder output = Json.createObjectBuilder();
        WriteOnceAddressSpace woas = new WriteOnceAddressSpace(currentView);
        try {
            byte[] data = woas.read(pos);
            output.add("state", "data");
            Object obj = null;
            try (ByteArrayInputStream bis = new ByteArrayInputStream(data))
            {
                try (ObjectInputStream ois = new ObjectInputStream(bis))
                {
                    obj = ois.readObject();
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
                                    log.debug("Getting field " + field.getName().toString());
                                    Object odata = field.get(obj);
                                    datan.add(field.getName().toString(), odata == null ? "null" :
                                                                          odata.toString() == null ? "null" :
                                                                          odata.toString());
                                    log.debug("Successfully added");
                                }
                            } catch (IllegalArgumentException iae) {}
                            catch (IllegalAccessException iae) {}
                        }
                    } while ((current = current.getSuperclass()) != null && current != Object.class);
                    log.debug("output!");
                    output.add("data", datan);
                }
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

        }
        catch (TrimmedException te)
        {
            output.add("state", "trimmed");
        }
        catch (UnwrittenException ue)
        {
            output.add("state", "unwritten");
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

            String response = null;

            if (t.getRequestMethod().startsWith("POST")) {
                log.debug("POST request:" + t.getRequestURI());
                String apiCall = null;
                JsonObject params = null;
                JsonNumber id = null;
                try (InputStreamReader isr  = new InputStreamReader(t.getRequestBody(), "utf-8"))
                {
                    try (BufferedReader br = new BufferedReader(isr))
                    {
                        try (JsonReader jr = Json.createReader(br))
                        {
                            JsonObject jo  = jr.readObject();
                            log.debug("request is " + jo.toString());
                            apiCall = jo.getString("method");
                            params = jo.getJsonObject("params");
                            id = jo.getJsonNumber("id");
                        }
                        catch (Exception e)
                        {
                            log.error("error", e);
                        }
                    }
                }
                Object result = JsonValue.FALSE;
                JsonObjectBuilder job = Json.createObjectBuilder();
                switch(apiCall)
                {
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
                }
                JsonObject res =    job.add("calledmethod", apiCall)
                                    .add("jsonrpc", "2.0")
                                    .add("id", id)
                                    .build();
                response = res.toString();
                log.debug("Response is " + response);
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
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();

        }
    }

}

