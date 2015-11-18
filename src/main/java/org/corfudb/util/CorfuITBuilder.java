package org.corfudb.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ICorfuDBServer;
import org.corfudb.infrastructure.NettyLayoutKeeper;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.configmasters.ILayoutKeeper;
import org.corfudb.runtime.view.*;

import javax.json.*;
import java.lang.reflect.Constructor;
import java.util.*;

@Slf4j
public class CorfuITBuilder {

    LinkedList<ICorfuDBServer> serverList;
    LinkedList<ICorfuDBServer> runningServers;

    JsonObjectBuilder layout = Json.createObjectBuilder();
    JsonArrayBuilder sequencerObject = Json.createArrayBuilder();
    JsonArrayBuilder configmasterObject = Json.createArrayBuilder();
    JsonArrayBuilder segmentObject = Json.createArrayBuilder();
    JsonArrayBuilder groups = Json.createArrayBuilder();
    JsonArrayBuilder groupObject = Json.createArrayBuilder(); // todo this should be a list

    int layoutKeeperPort;
    CorfuDBView view;

    @SuppressWarnings("unchecked")
    public CorfuITBuilder()
    {
        serverList = new LinkedList<ICorfuDBServer>();
        runningServers = new LinkedList<ICorfuDBServer>();
    }

    /**
     * Add a sequencer to this configuration at the specified port.
     * @param port      The port this sequencer will serve on.
     * @param sequencerType       The type of sequencer to instantiate.
     * @param clientProtocol    The type of protocol to advertise to clients.
     * @param baseParams        The parameters to initialize with, or null for none.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public CorfuITBuilder addSequencer(int port, Class<? extends ICorfuDBServer> sequencerType, String clientProtocol, Map<String,Object> baseParams)
    {
        Constructor<? extends ICorfuDBServer> serverConstructor = sequencerType.getConstructor();
        ICorfuDBServer server = serverConstructor.newInstance();
        Map<String, Object> params = baseParams == null ? new HashMap<>() : baseParams;
        params.put("port", port);
        serverList.add(server.getInstance(params));

        sequencerObject.add(clientProtocol + "://localhost:" + port);

        return this;
    }

    /**
     * Add a logging unit to the specified chain
     * @param port      The port this logunit will server on.
     * @param chain     The chain that this logunit will be attached to.
     * @param loggingType       The type of logging unit to instantiate.
     * @param clientProtocol    The type of protocol to advertise to clients.
     * @param baseParams        The parameters to initialize with, or null for none.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public CorfuITBuilder addLoggingUnit(int port, int chain, Class<? extends ICorfuDBServer> loggingType, String clientProtocol, Map<String,Object> baseParams)
    {
        Constructor<? extends ICorfuDBServer> serverConstructor = loggingType.getConstructor();
        ICorfuDBServer server = serverConstructor.newInstance();
        Map<String, Object> params = baseParams == null ? new HashMap<>() : baseParams;
        params.put("port", port);
        serverList.add(server.getInstance(params));

        groupObject.add(clientProtocol + "://localhost:" + port);

        return this;
    }

    /** construct a view from the parameters-map so far
     *
     * @param layoutKeeperPort     The port to run the LayoutKeeper on.
     */
    public CorfuITBuilder getView(int layoutKeeperPort) {

        this.layoutKeeperPort = layoutKeeperPort;
        configmasterObject.add("cdbmk://localhost:" + layoutKeeperPort);

        // segments
        //
        JsonObjectBuilder jsb = Json.createObjectBuilder();
        jsb.add("replication", "cdbcr");
        jsb.add("start", 0L);
        jsb.add("sealed", 0L);

        groups.add(groupObject); // todo list ..
        jsb.add("groups", groups);
        segmentObject.add(jsb);

        JsonObject layoutObject = layout
                .add("epoch", 0L)
                .add("logid", UUID.randomUUID().toString())
                .add("sequencer", sequencerObject)
                .add("configmaster", configmasterObject)
                .add("segments", segmentObject)
                .build();

        view = new CorfuDBView(layoutObject);

        return this;
    }


    /**
     * Start the system by initializing the LayoutKeeper at the specified port and running every component.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public CorfuDBView start()
    {
        Map<String, Object> params = new HashMap<>();
        params.put("port", layoutKeeperPort);

        ICorfuDBServer layoutKeeper = new NettyLayoutKeeper().getInstance(params);
        layoutKeeper.start();
        runningServers.add(layoutKeeper);
        Thread.sleep(1000);
        log.info("layout-keeper started...");

        // start all components
        //
        log.info("Starting all components...");
        serverList.forEach(rr -> {
            rr.start();
            runningServers.add(rr);
        });

        /* wait for all threads to start*/
        runningServers.forEach( th -> {
            if (!th.getThread().isAlive())
            {
                try {
                    Thread.sleep(1000); //don't want to hang, so just sleep 1s hope it'll come alive..
                } catch (InterruptedException ie) {}
                if (!th.getThread().isAlive())
                {
                    log.warn("Waited for 1s, but thread is still not alive!");
                }
            }
        });

        ((NettyLayoutKeeper)layoutKeeper).reconfig(view.getSerializedJSONView());
        // set bootstrap view
        // ( (ILayoutKeeper) view.getLayouts().get(0)).setBootstrapView(view.getSerializedJSONView());

        // start monitor thread
        //
        ICorfuDBInstance instance = new LocalCorfuDBInstance("localhost", layoutKeeperPort, view);
        new ViewMonitor(instance);

        return this.view;
    }

    /**
     * Factory class for getting an infrastructure builder.
     * @return  An infrastructure builder.
     */
    public static CorfuITBuilder getBuilder()
    {
        return new CorfuITBuilder();
    }

    /**
     * Shutdown servers and wait.
     */
    public void shutdownAndWait()
    {
        log.info("Shutting down dynamically created infrastructure...");
        runningServers.forEach(ICorfuDBServer::close);
    }
}
