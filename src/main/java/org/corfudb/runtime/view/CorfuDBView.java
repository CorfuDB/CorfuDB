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

package org.corfudb.runtime.view;

import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.configmasters.IMetaData;
import org.corfudb.runtime.protocols.replications.IReplicationProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;

import java.util.regex.Matcher;

import java.lang.reflect.Method;
import java.lang.StringBuilder;

import javax.json.Json;
import javax.json.JsonValue;
import javax.json.JsonString;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;

import org.corfudb.runtime.protocols.configmasters.IConfigMaster;

import java.util.UUID;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.corfudb.runtime.protocols.IServerProtocol;
/**
 * This class provides a view of the CorfuDB infrastructure. Clients
 * should not directly access the view without an interface.
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */

public class CorfuDBView {
    private static final Logger log = LoggerFactory.getLogger(CorfuDBView.class);

    private long epoch;
    private long pagesize;
    private UUID logID;
    private boolean isInvalid = false;
    private static Map<String,Class<? extends IServerProtocol>> availableSequencerProtocols= getSequencerProtocolClasses();
    private static Map<String,Class<? extends IServerProtocol>> availableLogUnitProtocols= getLogUnitProtocolClasses();
    private static Map<String,Class<? extends IServerProtocol>> availableConfigMasterProtocols = getConfigMasterProtocolClasses();
    private static Map<String,Class<? extends IReplicationProtocol>> availableReplicationProtocols = getReplicationProtocolClasses();

    private List<IServerProtocol> sequencers;
    private List<CorfuDBViewSegment> segments; //eventually this should be upgraded to rangemap or something..
    private List<IServerProtocol> configmasters;

    public CorfuDBView(JsonObject jsonView)
    {
        epoch = jsonView.getJsonNumber("epoch").longValue();
        pagesize = jsonView.getJsonNumber("pagesize").longValue();
        logID = UUID.fromString(jsonView.getJsonString("logid").getString());
        LinkedList<String> lsequencers = new LinkedList<String>();
        for (JsonValue j : jsonView.getJsonArray("sequencer"))
        {
            lsequencers.add(((JsonString)j).getString());
        }
        sequencers = populateSequencersFromList(lsequencers);
        LinkedList<String> lconfigmasters = new LinkedList<String>();
        for (JsonValue j : jsonView.getJsonArray("configmaster"))
        {
            lconfigmasters.add(((JsonString)j).getString());
        }
        configmasters = populateConfigMastersFromList(lconfigmasters);

        ArrayList<Map<String,Object>> lSegments = new ArrayList<Map<String,Object>>();
        for (JsonValue j : jsonView.getJsonArray("segments"))
        {
            JsonObject jo = (JsonObject) j;

            Map<String, Object> segmentMap = null;
            String replication = jo.getJsonString("replication").getString();
            if (replication == null) {
                // The default replication protocol is Chain Replication
                replication = "cdbcr";
            }
            if (!availableReplicationProtocols.keySet().contains(replication))
            {
                log.warn("Unsupported replication protocol: " + replication);
            }
            else
            {
                Class<? extends IReplicationProtocol> replicationClass = availableReplicationProtocols.get(replication);
                try
                {
                    segmentMap = (Map<String, Object>) replicationClass.getMethod("segmentParser", JsonObject.class).invoke(null, jo);
                }
                catch (Exception ex) {
                    log.error("Error invoking protocol for protocol: ", ex);
                }
            }

            segmentMap.put("replication", replication);
            segmentMap.put("start", jo.getJsonNumber("start").longValue());
            segmentMap.put("sealed", jo.getJsonNumber("sealed").longValue());
            lSegments.add(segmentMap);
        }
        segments = populateSegmentsFromList(lSegments);
    }

    /**
     * Get a CorfuDBView from a configuration object. This is used by
     * the configuration master to construct the inital view of the
     * system.
     *
     * @param config    A configuration object from the parsed yml file.
     */
    @SuppressWarnings("unchecked")
    public CorfuDBView(Map<String,Object> config)
    {
        if (config.containsKey("logid"))
        {
            logID = UUID.fromString((String) config.get("logid"));
        }
        epoch = config.get("epoch").getClass() == Long.class ? (Long) config.get("epoch") : (Integer) config.get("epoch");
        pagesize = config.get("pagesize").getClass() == Long.class ? (Long) config.get("pagesize") : (Integer) config.get("pagesize");
        sequencers = populateSequencersFromList((List<String>) config.get("sequencers"));
        configmasters = populateConfigMastersFromList((List<String>)config.get("configmasters"));
        segments = populateSegmentsFromList((List<Map<String,Object>>)((Map<String,Object>)config.get("layout")).get("segments"));
    }

    public void setUUID(UUID uuid)
    {
        this.logID = uuid;
    }

    public UUID getUUID()
    {
        return this.logID;
    }

    public long getEpoch()
    {
        return epoch;
    }

    /**
     * Attempts to move all servers in this view to the given epoch. This should be called by
     * the configuration master only!
     */
    public void setEpoch(long epoch)
    {
        for (IServerProtocol sequencer : sequencers)
        {
            sequencer.setEpoch(epoch);
        }

        for (CorfuDBViewSegment vs : segments)
        {
            for (List<IServerProtocol> group : vs.getGroups()) {
                for (IServerProtocol logunit : group) {
                    logunit.setEpoch(epoch);
                }
            }
        }
    }

    /**
     * Manually force the view into the given epoch. This should be called by the configuration
     * master only!
     *
     * @param epoch The epoch to move to
     */
    public void resetEpoch(long epoch)
    {
        this.epoch = epoch;
    }

    public List<IServerProtocol> getSequencers() {
        return sequencers;
    }

    public List<CorfuDBViewSegment> getSegments() {
        return segments;
    }

    public void moveAllToNewEpoch(long epoch)
    {
        this.epoch = epoch;
        this.getSegments().stream().forEach(segment ->
                segment.getGroups().stream().forEach(
                        group -> group.stream().forEach(
                                node -> node.setEpoch(epoch)
                        )
                ));

        this.getSequencers().stream().forEach(sequencer -> sequencer.setEpoch(epoch));
    }

    public List<IServerProtocol> getConfigMasters() {
        return configmasters;
    }

    /**
     * Checks if all servers in the view can be accessed. Does not check
     * to see if all the servers are in a valid configuration epoch.
     */
    public boolean isViewAccessible()
    {
        for (IServerProtocol sequencer : sequencers)
        {
            if (!sequencer.ping()) {
                log.debug("View acessibility check failed, couldn't connect to: " + sequencer.getFullString());
                return false;
            }
        }

        for (CorfuDBViewSegment vs : segments)
        {
            for (List<IServerProtocol> group : vs.getGroups())
            {
                for (IServerProtocol logunit: group)
                {
                    if (!logunit.ping()) {
                    log.debug("View acessibility check failed, couldn't connect to: " + logunit.getFullString());
                    return false;
                    }
                }
            }
        }

        return true;
    }

    public JsonObject getSerializedJSONView()
    {
        JsonArrayBuilder sequencerObject = Json.createArrayBuilder();
        for (IServerProtocol sp : sequencers)
        {
            sequencerObject.add(sp.getFullString());
        }

        JsonArrayBuilder configmasterObject = Json.createArrayBuilder();
        for (IServerProtocol sp : configmasters)
        {
            configmasterObject.add(sp.getFullString());
        }

        JsonArrayBuilder segmentObject = Json.createArrayBuilder();
        for (CorfuDBViewSegment vs: segments)
        {
            JsonObjectBuilder jsb = Json.createObjectBuilder();
            try {
                jsb.add("replication", (String) vs.getReplicationProtocol().getClass().getMethod("getProtocolString").invoke(null));
            } catch (Exception e) {
                log.warn("Couldn't add replication protocol string to serialized json");
            }

            jsb.add("start", vs.getStart());
            jsb.add("sealed", vs.getSealed());

            //TODO: Serialization will change depending on the replication protocol.

            JsonArrayBuilder groups = Json.createArrayBuilder();
            for (List<IServerProtocol> lsp : vs.getGroups())
            {
                JsonArrayBuilder group = Json.createArrayBuilder();
                for (IServerProtocol sp : lsp)
                {
                    group.add(sp.getFullString());
                }
                groups.add(group);
            }
            jsb.add("groups", groups);
            segmentObject.add(jsb);
        }

        return Json.createObjectBuilder()
                                .add("epoch", epoch)
                                .add("logid", logID.toString())
                                .add("pagesize", pagesize)
                                .add("sequencer", sequencerObject)
                                .add("configmaster", configmasterObject)
                                .add("segments", segmentObject)
                                .build();
    }

    @SuppressWarnings("unchecked")
    private List<CorfuDBViewSegment> populateSegmentsFromList(List<Map<String,Object>> list)
    {
        ArrayList<CorfuDBViewSegment> segments = new ArrayList<CorfuDBViewSegment>();
        for (Map<String,Object> m : list)
        {
            long start = m.get("start").getClass() == Long.class ? (Long) m.get("start") : (Integer) m.get("start");
            long sealed = m.get("sealed").getClass() ==  Long.class ? (Long) m.get("sealed") : (Integer) m.get("sealed");

            String replication = (String) m.get("replication");
            if (!availableReplicationProtocols.keySet().contains(replication))
            {
                log.warn("Unsupported replication protocol: " + replication);
            }
            else
            {
                Class<? extends IReplicationProtocol> replicationClass = availableReplicationProtocols.get(replication);
                IReplicationProtocol prot = null;
                try {
                    prot = (IReplicationProtocol) replicationClass.getMethod("initProtocol", Map.class, Map.class, Long.class).invoke(null, m, availableLogUnitProtocols, (Long) epoch);
                } catch (Exception e) {
                    log.warn("Cannot create a rep protocol with the given replication class: " + replicationClass + " error: " + e);
                }
                segments.add(new CorfuDBViewSegment(start, sealed, prot));
            }
        }
        return segments;
    }

    /**
     * Invalidate this view. This prevents this view from being used by
     * clients accessing the view through CorfuDBRuntime.
     */
    public void invalidate()
    {
        isInvalid = true;
    }

    /**
     * Revalidate this view. This allows this view to be used by
     * clients accessing the view through CorfuDBRuntime.
     */
    public void revalidate()
    {
        isInvalid = false;
    }


    /**
     * Check if the view is valid.
     *
     * @return True if the view is valid, false otherwise.
     */
    public boolean isValid()
    {
        return !isInvalid;
    }

    @SuppressWarnings("unchecked")
    private List<List<IServerProtocol>> populateGroupsFromList(List<Map<String,Object>> list) {
        ArrayList<List<IServerProtocol>> groups = new ArrayList<List<IServerProtocol>>();
        for (Map<String,Object> map : list)
        {
            ArrayList<IServerProtocol> nodes = new ArrayList<IServerProtocol>();
            for (String node : (List<String>)map.get("nodes"))
            {
                Matcher m = IServerProtocol.getMatchesFromServerString(node);
                if (m.find())
                {
                    String protocol = m.group("protocol");
                    if (!availableLogUnitProtocols.keySet().contains(protocol))
                    {
                        log.warn("Unsupported logunit protocol: " + protocol);
                    }
                    else
                    {
                        Class<? extends IServerProtocol> sprotocol = availableLogUnitProtocols.get(protocol);
                        try
                        {
                            nodes.add(IServerProtocol.protocolFactory(sprotocol, node, epoch));
                        }
                        catch (Exception ex){
                            log.error("Error invoking protocol for protocol: ", ex);
                        }
                    }
                }
                else
                {
                    log.warn("Logunit string " + node + " appears to be an invalid logunit string");
                }
            }
            groups.add(nodes);
        }
        return groups;
    }


    private List<IServerProtocol> populateSequencersFromList(List<String> list) {
        LinkedList<IServerProtocol> sequencerList = new LinkedList<IServerProtocol>();
        for (String s : list)
        {
            Matcher m = IServerProtocol.getMatchesFromServerString(s);
            if (m.find())
            {
                String protocol = m.group("protocol");
                if (!availableSequencerProtocols.keySet().contains(protocol))
                {
                    log.warn("Unsupported sequencer protocol: " + protocol);
                }
                else
                {
                    Class<? extends IServerProtocol> sprotocol = availableSequencerProtocols.get(protocol);
                    try
                    {
                        sequencerList.add(IServerProtocol.protocolFactory(sprotocol, s, epoch));
                    }
                    catch (Exception ex){
                        log.error("Error invoking protocol for protocol: ", ex);
                    }
                }
            }
            else
            {
                log.warn("Sequencer string " + s + " appears to be an invalid sequencer string");
            }
        }
        return sequencerList;
    }

    private List<IServerProtocol> populateConfigMastersFromList(List<String> list) {
        LinkedList<IServerProtocol> sequencerList = new LinkedList<IServerProtocol>();
        for (String s : list)
        {
            Matcher m = IServerProtocol.getMatchesFromServerString(s);
            if (m.find())
            {
                String protocol = m.group("protocol");
                if (!availableConfigMasterProtocols.keySet().contains(protocol))
                {
                    log.warn("Unsupported config master protocol: " + protocol);
                }
                else
                {
                    Class<? extends IServerProtocol> sprotocol = availableConfigMasterProtocols.get(protocol);
                    try
                    {
                        sequencerList.add(IServerProtocol.protocolFactory(sprotocol, s, epoch));
                    }
                    catch (Exception ex){
                        log.error("Error invoking protocol for protocol: ", ex);
                    }
                }
            }
            else
            {
                log.warn("Configmaster string " + s + " appears to be an invalid configmaster string");
            }
        }
        return sequencerList;
    }

    public static IMetaData getConfigurationMasterFromString(String masterString)
    {
        Matcher m = IServerProtocol.getMatchesFromServerString(masterString);
            if (m.find())
            {
                String protocol = m.group("protocol");
                if (!availableConfigMasterProtocols.keySet().contains(protocol))
                {
                    log.warn("Unsupported config master protocol: " + protocol);
                }
                else
                {
                    Class<? extends IServerProtocol> sprotocol = availableConfigMasterProtocols.get(protocol);
                    try
                    {
                        return (IMetaData) IServerProtocol.protocolFactory(sprotocol, masterString, 0);
                    }
                    catch (Exception ex){
                        log.error("Error invoking protocol for protocol: ", ex);
                    }
                }
            }
            else
            {
                log.warn("Configmaster string " + masterString + " appears to be an invalid configmaster string");
            }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String,Class<? extends IServerProtocol>> getSequencerProtocolClasses()
    {
        Reflections reflections = new Reflections("org.corfudb.runtime.protocols.sequencers", new SubTypesScanner(false));
        Set<Class<? extends Object>> allClasses = reflections.getSubTypesOf(Object.class);
        Map<String, Class<? extends IServerProtocol>> sequencerMap = new HashMap<String,Class<? extends IServerProtocol>>();

        for(Class<? extends Object> c : allClasses)
        {
            try {
                Method getProtocolString = c.getMethod("getProtocolString");
                String protocol = (String) getProtocolString.invoke(null);
                sequencerMap.put(protocol, (Class<? extends IServerProtocol>)c);
            }
            catch (Exception e)
            {
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Found ").append(sequencerMap.size()).append(" supported sequencer(s):\n");
        for(String proto : sequencerMap.keySet())
        {
            sb.append(proto + "\t\t- " + sequencerMap.get(proto).toString() + "\n");
        }
        log.debug(sb.toString());
        return sequencerMap;
    }

    @SuppressWarnings("unchecked")
    private static Map<String,Class<? extends IServerProtocol>> getLogUnitProtocolClasses()
    {
        Reflections reflections = new Reflections("org.corfudb.runtime.protocols.logunits", new SubTypesScanner(false));
        Set<Class<? extends Object>> allClasses = reflections.getSubTypesOf(Object.class);
        Map<String, Class<? extends IServerProtocol>> logunitMap = new HashMap<String,Class<? extends IServerProtocol>>();

        for(Class<? extends Object> c : allClasses)
        {
            try {
                Method getProtocolString = c.getMethod("getProtocolString");
                String protocol = (String) getProtocolString.invoke(null);
                logunitMap.put(protocol, (Class<? extends IServerProtocol>)c);
            }
            catch (Exception e)
            {
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Found ").append(logunitMap.size()).append(" supported stream unit(s):\n");
        for(String proto : logunitMap.keySet())
        {
            sb.append(proto + "\t\t- " + logunitMap.get(proto).toString() + "\n");
        }
        log.debug(sb.toString());
        return logunitMap;
    }

    @SuppressWarnings("unchecked")
    private static Map<String,Class<? extends IServerProtocol>> getConfigMasterProtocolClasses()
    {
        Reflections reflections = new Reflections("org.corfudb.runtime.protocols.configmasters", new SubTypesScanner(false));
        Set<Class<? extends Object>> allClasses = reflections.getSubTypesOf(Object.class);
        Map<String, Class<? extends IServerProtocol>> logunitMap = new HashMap<String,Class<? extends IServerProtocol>>();

        for(Class<? extends Object> c : allClasses)
        {
            try {
                Method getProtocolString = c.getMethod("getProtocolString");
                String protocol = (String) getProtocolString.invoke(null);
                logunitMap.put(protocol, (Class<? extends IServerProtocol>)c);
            }
            catch (Exception e)
            {
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Found ").append(logunitMap.size()).append(" supported configuration master(s):\n");
        for(String proto : logunitMap.keySet())
        {
            sb.append(proto + "\t\t- " + logunitMap.get(proto).toString() + "\n");
        }
        log.debug(sb.toString());
        return logunitMap;
    }

    @SuppressWarnings("unchecked")
    private static Map<String,Class<? extends IReplicationProtocol>> getReplicationProtocolClasses()
    {
        Reflections reflections = new Reflections("org.corfudb.runtime.protocols.replications", new SubTypesScanner(false));
        Set<Class<? extends Object>> allClasses = reflections.getSubTypesOf(Object.class);
        Map<String, Class<? extends IReplicationProtocol>> replicationMap = new HashMap<String,Class<? extends IReplicationProtocol>>();

        for(Class<? extends Object> c : allClasses)
        {
            try {
                Method getProtocolString = c.getMethod("getProtocolString");
                String protocol = (String) getProtocolString.invoke(null);
                replicationMap.put(protocol, (Class<? extends IReplicationProtocol>)c);
            }
            catch (Exception e)
            {
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Found ").append(replicationMap.size()).append(" supported replication protocol(s):\n");
        for(String proto : replicationMap.keySet())
        {
            sb.append(proto + "\t\t- " + replicationMap.get(proto).toString() + "\n");
        }
        log.debug(sb.toString());
        return replicationMap;
    }
}
