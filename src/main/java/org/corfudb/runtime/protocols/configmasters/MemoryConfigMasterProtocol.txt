package org.corfudb.runtime.protocols.configmasters;

import org.corfudb.infrastructure.configmaster.policies.IReconfigurationPolicy;
import org.corfudb.infrastructure.configmaster.policies.SimpleReconfigurationPolicy;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.view.IReconfigurationPolicy;
import org.corfudb.runtime.view.SimpleReconfigurationPolicy;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.MemoryLogUnitProtocol;
import org.corfudb.runtime.protocols.sequencers.MemorySequencerProtocol;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.StreamData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonObject;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mwei on 4/30/15.
 */
public class MemoryConfigMasterProtocol implements ILayoutKeeper, IServerProtocol {

    private Logger log = LoggerFactory.getLogger(MemoryConfigMasterProtocol.class);
    private Map<String,String> options;
    private String host;
    private Integer port;
    private Long epoch;
    private CorfuDBView view;
    private CorfuDBView initialView;
    private IReconfigurationPolicy reconfigPolicy;
    private UUID logID;

    public static ConcurrentHashMap<Integer, MemoryConfigMasterProtocol> memoryConfigMasters =
            new ConcurrentHashMap<Integer, MemoryConfigMasterProtocol>();

    public MemoryConfigMasterProtocol() {
        this("localhost", 0, new HashMap<String,String>(), 0L);
    }

    public static String getProtocolString()
    {
        return "mcm";
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        IServerProtocol res;
        if ((res = memoryConfigMasters.get(port)) != null)
        {
            return res;
        }
        return new MemoryConfigMasterProtocol(host, port, options, epoch);
    }

    public MemoryConfigMasterProtocol(String host, Integer port, Map<String,String> options, Long epoch)
    {
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;
        this.reconfigPolicy = new SimpleReconfigurationPolicy();
        memoryConfigMasters.put(this.port, this);
    }

  public CompletableFuture<Boolean> proposeNewView(int rank, JsonObject jo) {
      return null;
  }

    /**
     * Gets the current view from the configuration master.
     * @return  The current view.
     */
    public CompletableFuture<JsonObject> getCurrentView() { return null; }

    /**
     * sets a bootstrap view at a particular MetaDataKeeper unit
     * @param initialView the initial view
     */
    public void setBootstrapView(JsonObject initialView) { setInitialView(new CorfuDBView(initialView));}

    public void setInitialView(CorfuDBView view)
    {
        this.view = view;
        logID = UUID.randomUUID();
        this.view.setLogID(logID);
        this.initialView = view;
    }

    /** Resets (removes) all in memory units from view */
    public static void inMemoryClear()
    {
        MemoryConfigMasterProtocol.memoryConfigMasters.clear();
        MemoryLogUnitProtocol.memoryUnits.clear();
        MemorySequencerProtocol.memorySequencers.clear();
    }

    /**
     * Gets the current view from the configuration master.
     *
     * @return The current view.
     */
    @Override
    public CorfuDBView getView() {
        return view;
    }

    /**
     * Returns the full server string.
     *
     * @return A server string.
     */
    @Override
    public String getFullString() {
        return "mcm";
    }

    /**
     * Returns the host
     *
     * @return The hostname for the server.
     */
    @Override
    public String getHost() {
        return host;
    }

    /**
     * Returns the port
     *
     * @return The port number of the server.
     */
    @Override
    public Integer getPort() {
        return port;
    }

    /**
     * Returns the option map
     *
     * @return The option map for the server.
     */
    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    /**
     * Returns a boolean indicating whether or not the server was reachable.
     *
     * @return True if the server was reachable, false otherwise.
     */
    @Override
    public boolean ping() {
        return true;
    }

    /**
     * Sets the epoch of the server. Used by the configuration master to switch epochs.
     *
     * @param epoch
     */
    @Override
    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    /**
     * Resets the server. Used by the configuration master to reset the state of the server.
     * Should eliminate ALL hard state!
     *
     * @param epoch
     */
    @Override
    public void reset(long epoch) throws NetworkException {
        this.view = this.initialView;
        logID = UUID.randomUUID();
        this.view.setLogID(logID);
        this.initialView.setEpoch(epoch);
    }
}
