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

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.configmaster.policies.IReconfigurationPolicy;
import org.corfudb.infrastructure.configmaster.policies.SimpleReconfigurationPolicy;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyLayoutServerRequestMsg;
import org.corfudb.infrastructure.wireprotocol.NettyLayoutServerResponseMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenRequestMsg;
import org.corfudb.runtime.NetworkException;
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

import org.corfudb.runtime.stream.Timestamp;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.runtime.view.RemoteLogView;
import org.corfudb.runtime.RemoteException;
import org.corfudb.runtime.view.StreamData;

import org.corfudb.runtime.view.CachedWriteOnceAddressSpace;
import org.corfudb.runtime.view.Serializer;

import java.util.concurrent.CompletableFuture;

public class LayoutServer extends AbstractNettyServer implements ICorfuDBServer {
    private static Logger log = LoggerFactory.getLogger(LayoutServer.class);

    private Map<String, Object> config;
    private CorfuDBView currentView;
    private StreamView currentStreamView;
    private Boolean viewActive;

    private Boolean running;
    @Getter

    private RemoteLogView currentRemoteView;
    int masterid = new SecureRandom().nextInt();

    public LayoutServer() {
    }

    @Override
    void parseConfiguration(Map<String, Object> configuration) {
        log.info("LayoutServer configuration {}", configuration);
        this.config = configuration;
        viewActive = false;
        currentView = new CorfuDBView(config);
        currentStreamView = new StreamView();
        currentRemoteView = new RemoteLogView();
        //UUID is going to be random for now, since this configuration is not persistent
        UUID logID = UUID.randomUUID();
        log.info("New log instance id= " + logID.toString());
        currentView.setUUID(logID);
    }

    @Override
    public void processMessage(NettyCorfuMsg corfuMsg, ChannelHandlerContext ctx) {

        log.info("Received request of type {}", corfuMsg.getMsgType());
        switch (corfuMsg.getMsgType())
        {
            case LAYOUT_REQ:
                NettyLayoutServerResponseMsg resp = new NettyLayoutServerResponseMsg(currentView.getSerializedJSONView());
                log.info("layout response {}", resp);
                sendResponse(resp, corfuMsg, ctx);
                break;
            default:
                break;
        }
    }


    @Override
    public void close() {

        running = false;
        /* if (server != null) {
             server.stop(0);
        }*/
        this.getThread().interrupt();
    }

    public void loadRemoteLogs() {
        if (config.get("remotelogs") != null) {
            for (String configMaster : (List<String>) config.get("remotelogs")) {
                try {
                    UUID remoteID = currentRemoteView.addLog(configMaster, currentView.getUUID());
                    CorfuDBView view = currentRemoteView.getLog(remoteID);
                    if (view != null) {
                        IConfigMaster cm = (IConfigMaster) view.getConfigMasters().get(0);
                        Map<UUID, String> remoteLogList = cm.getAllLogs();
                        for (UUID rlog : remoteLogList.keySet()) {
                            if (!rlog.equals(currentView.getUUID())) {
                                if (currentRemoteView.addLog(rlog, remoteLogList.get(rlog))) {
                                    log.info("Discovered new remote stream " + rlog);
                                }
                            }
                        }
                        //Tell the remote stream that we exist
                        cm.addLog(currentView.getUUID(), currentView.getConfigMasters().get(0).getFullString());
                    }
                } catch (Exception e) {
                    log.warn("Error talking to remote stream", e);
                }
            }
        }
    }

    public void checkViewThread() {
        log.info("Starting view check thread");
        while (running) {
            try {
                boolean success = currentView.isViewAccessible();
                if (success && !viewActive) {
                    log.info("New view is now accessible and active");
                    currentView.setEpoch(0);
                    viewActive = true;
                    synchronized (viewActive) {
                        viewActive.notify();
                    }
                } else if (!success) {
                    log.info("View is not accessible, checking again in 30s");
                }
                //also check if all remote logs are still accessible.
                //and remove any which are not.
                currentRemoteView.checkAllLogs();
                loadRemoteLogs();

                synchronized (viewActive) {
                    viewActive.wait(30000);
                }
            } catch (InterruptedException ie) {

            }
        }
    }

    @Override
    public void reset() {
        log.info("RESET requested, resetting all nodes and incrementing epoch");
        long newEpoch = currentView.getEpoch() + 1;

        for (IServerProtocol sequencer : currentView.getSequencers()) {
            try {
                sequencer.reset(newEpoch);
            } catch (Exception e) {
                log.error("Error resetting sequencer", e);
            }
        }

        for (CorfuDBViewSegment vs : currentView.getSegments()) {
            for (List<IServerProtocol> group : vs.getGroups()) {
                for (IServerProtocol logunit : group) {
                    try {
                        logunit.reset(newEpoch);
                    } catch (Exception e) {
                        log.error("Error resetting logunit", e);
                    }
                }
            }
        }

        currentStreamView = new StreamView();
        //UUID is going to be random for now, since this configuration is not persistent
        UUID logID = UUID.randomUUID();
        currentView.setUUID(logID);
        log.info("New log instance id= " + logID.toString());
        currentView.resetEpoch(newEpoch);
    }

    private JsonValue addStream(JsonObject params) {
        try {
            JsonObject jo = params;
            StreamData sd = currentStreamView.getStream(UUID.fromString(jo.getJsonString("streamid").getString()));
            boolean didnotalreadyexist = false;
            if (sd == null) {
                didnotalreadyexist = true;
            }
            currentStreamView.addStream(UUID.fromString(jo.getJsonString("streamid").getString()), UUID.fromString(jo.getJsonString("logid").getString()), jo.getJsonNumber("startpos").longValue());
            sd = currentStreamView.getStream(UUID.fromString(jo.getJsonString("streamid").getString()));

            if (didnotalreadyexist) {
                log.info("Adding new stream {}", sd.streamID);
                if (!(Boolean) jo.getBoolean("nopass", false)) {

                    for (UUID remote : currentRemoteView.getAllLogs()) {
                        CompletableFuture.runAsync(() -> {
                            try {
                                log.info("send addstream to {}", remote);
                                CorfuDBView cv = (CorfuDBView) currentRemoteView.getLog(remote);
                                IConfigMaster cm = (IConfigMaster) cv.getConfigMasters().get(0);
                                cm.addStreamCM(UUID.fromString(jo.getJsonString("logid").getString()), UUID.fromString(jo.getJsonString("streamid").getString()), jo.getJsonNumber("startpos").longValue(), true);
                            } catch (Exception e) {
                                log.debug("Error Broadcasting Gossip", e);
                            }
                        });
                    }
                }
            }
        } catch (Exception ex) {
            log.error("Error adding stream", ex);
            return JsonValue.FALSE;
        }
        return JsonValue.TRUE;
    }

    private JsonObject getStream(JsonObject params) {
        JsonObjectBuilder ob = Json.createObjectBuilder();

        try {
            JsonObject jo = params;
            log.debug(jo.toString());
            StreamData sd = currentStreamView.getStream(UUID.fromString(jo.getJsonString("streamid").getString()));
            if (sd == null) {
                ob.add("present", false);
            } else {
                ob.add("present", true);
                ob.add("currentlog", sd.currentLog.toString());
                ob.add("startlog", sd.startLog.toString());
                ob.add("startpos", sd.startPos);
                ob.add("epoch", sd.epoch);
            }

        } catch (Exception ex) {
            log.error("Error getting stream", ex);
            ob.add("present", false);
        }
        return ob.build();
    }

    private JsonObject getAllLogs(JsonObject params) {
        JsonObjectBuilder jb = Json.createObjectBuilder();
        Map<UUID, String> logs = currentRemoteView.getAllLogsMappings();
        for (UUID key : logs.keySet()) {
            jb.add(key.toString(), logs.get(key));
        }
        return jb.build();
    }

    private JsonArray streamInfo(JsonObject params) {
        JsonArrayBuilder jb = Json.createArrayBuilder();
        Set<UUID> streams = currentStreamView.getAllStreams();
        try {
            for (UUID key : streams) {
                StreamData sd = currentStreamView.getStream(key);
                JsonObjectBuilder job = Json.createObjectBuilder();
                job.add("streamid", sd.streamID.toString());
                job.add("currentlog", sd.currentLog.toString());
                job.add("startlog", sd.startLog.toString());
                job.add("startpos", sd.startPos);
                job.add("epoch", sd.epoch);
                jb.add(job);
            }
        } catch (Exception ex) {
            log.debug("Exception getting streaminfo", ex);
        }
        return jb.build();
    }

    private String getLog(JsonObject params) {
        try {
            return currentRemoteView.getLogString(UUID.fromString(params.getString("logid")));
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private JsonValue addLog(JsonObject params) {
        if (currentRemoteView.addLog(UUID.fromString(params.getString("logid")), params.getString("path"))) {
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

    private JsonObject streamInspector(JsonObject params) {
        long pos = params.getJsonNumber("pos").longValue();
        JsonObjectBuilder output = Json.createObjectBuilder();
        WriteOnceAddressSpace woas = new WriteOnceAddressSpace(currentView);
        return null;
    }

    private JsonObject reconfig(JsonObject params) {
        try {
            log.warn("Reconfiguration requested!");

            NetworkException ne = null;
            if (params.getJsonString("exception") != null) {
                String ex = params.getJsonString("exception").getString();
                ne = (NetworkException) Serializer.deserialize_compressed(Base64.getDecoder().decode(ex));
                log.warn("Reconfigure due to " + ne.getMessage());
            }
            IReconfigurationPolicy policy = new SimpleReconfigurationPolicy();
            currentView = policy.getNewView(currentView, ne);
            log.warn("Reconfiguration completed, view is now " + currentView.getSerializedJSONView().toString());
        } catch (Exception e) {
            log.warn("Exception", e);
        }
        return null;
    }

    private JsonObject newView(JsonObject params) {
        try {
            log.warn("Forcibly installing new view");
            log.warn("newview= " + params.getJsonString("newview").getString());
            try (StringReader isr = new StringReader(params.getJsonString("newview").getString())) {
                try (BufferedReader br = new BufferedReader(isr)) {
                    try (JsonReader jr = Json.createReader(br)) {
                        JsonObject jo = jr.readObject();
                        currentView = new CorfuDBView(jo);
                        ;
                    } catch (Exception e) {
                        log.error("error", e);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("exception", e);
        }
        return null;
    }

    private JsonObject logInfo(JsonObject params) {
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
            do {
                Field[] fields = current.getDeclaredFields();
                for (Field field : fields) {
                    try {
                        if (!Modifier.isTransient(field.getModifiers())) {
                            if (field.getName().toString().equals("ts")) {
                                Timestamp ts = (Timestamp) field.get(obj);
                                if (ts != null) {
                                    ts.physicalPos = pos;
                                }
                            } else if (field.getName().toString().equals("payload") && field.get(obj) != null) {
                                try {
                                    JsonObjectBuilder pdatan = Json.createObjectBuilder();
                                    Object pobj = field.get(obj);
                                    pdatan.add("classname", pobj.getClass().getName());
                                    Class<?> pcurrent = pobj.getClass();
                                    try {
                                        pdatan.add("string", pobj.toString());
                                    } catch (Exception ex) {
                                    }
                                    do {
                                        Field[] pfields = pcurrent.getDeclaredFields();
                                        for (Field pfield : pfields) {
                                            try {
                                                Object podata = pfield.get(obj);
                                                pdatan.add(pfield.getName().toString(), podata == null ? "null" :
                                                        podata.toString() == null ? "null" :
                                                                podata.toString());
                                            } catch (IllegalArgumentException iae) {
                                            } catch (IllegalAccessException iae) {
                                            }
                                        }
                                    } while ((pcurrent = pcurrent.getSuperclass()) != null && pcurrent != Object.class);
                                    output.add("payload", pdatan);
                                } catch (Exception e) {
                                    log.debug("Exception reading payload", e);
                                }
                            }
                            Object odata = field.get(obj);
                            datan.add(field.getName().toString(), odata == null ? "null" :
                                    odata.toString() == null ? "null" :
                                            odata.toString());
                        }
                    } catch (IllegalArgumentException iae) {
                    } catch (IllegalAccessException iae) {
                    }
                }
            } while ((current = current.getSuperclass()) != null && current != Object.class);
            output.add("data", datan);
        } catch (TrimmedException te) {
            output.add("state", "trimmed");
        } catch (UnwrittenException ue) {
            output.add("state", "unwritten");
        } catch (IOException ie) {
            output.add("classname", "unknown");
            output.add("error", ie.getMessage());

        } catch (ClassNotFoundException cnfe) {
            output.add("classname", "unknown");
            output.add("error", cnfe.getMessage());
        }

        return output.build();
    }

    /**
     * Created by dmalkhi on 11/5/15.
     */
    public class LayoutConsensus {

        private int highPhase1Rank;
        private int highPhase2Rank;
        private Proposal highPhase2Proposal;

        int thisEpoch;

        @Getter
        @Setter
        class Proposal {
            int nextEpoch;
            int rank;
            // currentView
            // nextView
        }

        Proposal getHighPhase2Proposal(int rank) {
            if (rank > highPhase1Rank) {
                highPhase1Rank = rank;
                return highPhase2Proposal;
            } else {
                return null;
            }
        }


        /**
         * @param rank
         * @param proposal
         * @return 0 means proposal accepted, -1 means it is rejected
         */
        int putHighPhase2Proposal(int rank, Proposal proposal) {
            if (rank >= highPhase1Rank) {
                // accept proposal
                highPhase2Rank = highPhase1Rank = rank;
                highPhase2Proposal = proposal;
                // todo should we learn from the proposal if a higher epoch has been installed by other layout servers already?
                return 0;
            } else {
                return -1;
            }
        }
    /*
        void proposeChange(view newView) {
            Proposal myProposal = new Proposal();
            myProposal.setRank(highPhase1Rank+1);
            int adoptRank = -1;
            boolean isLeader = true;
            boolean isDone = false;

            while (! done) {
                for (ls : layoutServers) {
                    CompletableFuture f = ls.getHighPhase2Proposal(myProposal.getRank());
                    // TODO use completion to increment a completion counter, and to collect responses or process
                }

                // todo wait for counter to reach quorum size
                //

                // obtain the results from the future
                //
                Proposal readProposal = lst.get().result;

                if (readProposal == null) {
                        newRank++;
                        break;
                    }

                    // check that myProposal epoch is highest than last installed
                    //
                    if (myProposal.epoch < readProposal.epoch) {
                        // TODO something
                    }

                    if (highPhase2Proposal.next != null && highPhase2Proposal.nextRank > adoptRank) {
                        adoptRank = highPhase2Rank.nextRank;
                        myProposal = readProposal;
                    }
                }
        }
        */
    }
}
