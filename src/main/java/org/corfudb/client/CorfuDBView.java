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

package org.corfudb.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.StringBuilder;

import javax.json.Json;
import javax.json.JsonValue;
import javax.json.JsonString;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

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
    private boolean isInvalid = false;
    private static Map<String,Class<? extends IServerProtocol>> availableSequencerProtocols= getSequencerProtocolClasses();
    private static Map<String,Class<? extends IServerProtocol>> availableLogUnitProtocols= getLogUnitProtocolClasses();

    private List<IServerProtocol> sequencers;
    private List<CorfuDBViewSegment> segments; //eventually this should be upgraded to rangemap or something..

    public CorfuDBView(JsonObject jsonView)
    {
        epoch = jsonView.getJsonNumber("epoch").longValue();
        pagesize = jsonView.getJsonNumber("pagesize").longValue();
        LinkedList<String> lsequencers = new LinkedList<String>();
        for (JsonValue j : jsonView.getJsonArray("sequencer"))
        {
            lsequencers.add(((JsonString)j).getString());
        }
        sequencers = populateSequencersFromList(lsequencers);
        ArrayList<Map<String,Object>> lSegments = new ArrayList<Map<String,Object>>();
        for (JsonValue j : jsonView.getJsonArray("segments"))
        {
            JsonObject jo = (JsonObject) j;
            HashMap<String,Object> tMap = new HashMap<String,Object>();
            tMap.put("start", jo.getJsonNumber("start").longValue());
            tMap.put("sealed", jo.getJsonNumber("sealed").longValue());
            ArrayList<Map<String, Object>> groupList = new ArrayList<Map<String, Object>>();
            for (JsonValue j2 : jo.getJsonArray("groups"))
            {
                HashMap<String,Object> groupItem = new HashMap<String,Object>();
                JsonArray ja = (JsonArray) j2;
                ArrayList<String> group = new ArrayList<String>();
                for (JsonValue j3 : ja)
                {
                    group.add(((JsonString)j3).getString());
                }
                groupItem.put("nodes", group);
                groupList.add(groupItem);
            }
            tMap.put("groups", groupList);
            lSegments.add(tMap);
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
        epoch = config.get("epoch").getClass() == Long.class ? (Long) config.get("epoch") : (Integer) config.get("epoch");
        pagesize = config.get("pagesize").getClass() == Long.class ? (Long) config.get("pagesize") : (Integer) config.get("pagesize");
        sequencers = populateSequencersFromList((List<String>)config.get("sequencers"));
        segments = populateSegmentsFromList((List<Map<String,Object>>)((Map<String,Object>)config.get("layout")).get("segments"));
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
            for (List<IServerProtocol> group : vs.getGroups())
            {
                for (IServerProtocol logunit: group)
                {
                    logunit.setEpoch(epoch);
                }
            }
        }

    }

    public List<IServerProtocol> getSequencers() {
        return sequencers;
    }

    public List<CorfuDBViewSegment> getSegments() {
        return segments;
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

        JsonArrayBuilder segmentObject = Json.createArrayBuilder();
        for (CorfuDBViewSegment vs: segments)
        {
            log.debug("found segment");
            JsonObjectBuilder jsb = Json.createObjectBuilder();
            jsb.add("start", vs.getStart());
            jsb.add("sealed", vs.getSealed());

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
                                .add("pagesize", pagesize)
                                .add("sequencer", sequencerObject)
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
            CorfuDBViewSegment vs = new CorfuDBViewSegment(start, sealed, populateGroupsFromList((List<Map<String,Object>>) m.get("groups")));
            segments.add(vs);
        }
        return segments;
    }

    /**
     * Invalidate this view. This prevents this view from being used by
     * clients accessing the view through CorfuDBClient.
     */
    void invalidate()
    {
        isInvalid = true;
    }

    /**
     * Check if the view is valid.
     *
     * @return True if the view is valid, false otherwise.
     */
    boolean isValid()
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

    @SuppressWarnings("unchecked")
    private static Map<String,Class<? extends IServerProtocol>> getSequencerProtocolClasses()
    {
        Reflections reflections = new Reflections("org.corfudb.client.sequencers", new SubTypesScanner(false));
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
        Reflections reflections = new Reflections("org.corfudb.client.logunits", new SubTypesScanner(false));
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
        sb.append("Found ").append(logunitMap.size()).append(" supported log unit(s):\n");
        for(String proto : logunitMap.keySet())
        {
            sb.append(proto + "\t\t- " + logunitMap.get(proto).toString() + "\n");
        }
        log.debug(sb.toString());
        return logunitMap;
    }
}
