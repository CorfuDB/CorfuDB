package org.corfudb.runtime.protocols.configmasters;

import org.corfudb.infrastructure.configmaster.policies.IReconfigurationPolicy;
import org.corfudb.infrastructure.configmaster.policies.SimpleReconfigurationPolicy;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.MemoryLogUnitProtocol;
import org.corfudb.runtime.protocols.sequencers.MemorySequencerProtocol;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.StreamData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mwei on 4/30/15.
 */
public class MemoryConfigMasterProtocol implements IConfigMaster, IServerProtocol {

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

    public void setInitialView(CorfuDBView view)
    {
        this.view = view;
        logID = UUID.randomUUID();
        this.view.setUUID(logID);
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
     * Adds a new stream to the system.
     *
     * @param logID    The ID of the stream the stream starts on.
     * @param streamID The streamID of the stream.
     * @param startPos The start position of the stream.
     * @return True if the stream was successfully added to the system, false otherwise.
     */
    @Override
    public boolean addStream(UUID logID, UUID streamID, long startPos) {
        return false;
    }

    /**
     * Adds a new stream to the system.
     *
     * @param logID    The ID of the stream the stream starts on.
     * @param streamID The streamID of the stream.
     * @param startPos The start position of the stream.
     * @param nopass
     * @return True if the stream was successfully added to the system, false otherwise.
     */
    @Override
    public boolean addStreamCM(UUID logID, UUID streamID, long startPos, boolean nopass) {
        return false;
    }

    /**
     * Gets information about a stream in the system.
     *
     * @param streamID The ID of the stream to retrieve.
     * @return A StreamData object containing information about the stream, or null if the
     * stream could not be found.
     */
    @Override
    public StreamData getStream(UUID streamID) {
        return null;
    }

    /**
     * Adds a stream to the system.
     *
     * @param logID The ID of the stream to add.
     * @param path  True, if the stream was added to the system, or false otherwise.
     */
    @Override
    public boolean addLog(UUID logID, String path) {
        return false;
    }

    /**
     * Gets information about all logs known to the system.
     *
     * @return A map containing all logs known to the system.
     */
    @Override
    public Map<UUID, String> getAllLogs() {
        return null;
    }

    /**
     * Gets the configuration string for a stream, given its id.
     *
     * @param logID The ID of the stream to retrieve.
     * @return The configuration string used to access that stream.
     */
    @Override
    public String getLog(UUID logID) {
        return null;
    }


    /**
     * Resets the entire stream, and increments the epoch. Use only during testing to restore the system to a
     * known state.
     */
    @Override
    public void resetAll() {
        /* just reset everything in memory */
        log.info("Request reset of memory configuration");
        try {
            this.reset(epoch + 1);
        } catch (Exception e) {
            log.error("Error during configmaster reset", e);
        }
        for (MemorySequencerProtocol msp : MemorySequencerProtocol.memorySequencers.values())
        {
            try {
            msp.reset(epoch+1);}
            catch (Exception e) {
                log.error("Error during sequencer reset", e);
            }
        }
        for (MemoryLogUnitProtocol mlu : MemoryLogUnitProtocol.memoryUnits.values())
        {
            try {
                mlu.reset(epoch+1);}
            catch (Exception e) {
                log.error("Error during log unit reset", e);
            }
        }
        this.epoch++;
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
     * Request reconfiguration due to a network exception.
     *
     * @param e The network exception that caused the reconfiguration request.
     */
    @Override
    public void requestReconfiguration(NetworkException e) {
        this.view = reconfigPolicy.getNewView(this.view, e);
        log.info("Reconfiguration requested, moving to new view epoch " + this.view.getEpoch());
    }

    /**
     * Force the configuration master to install a new view. This method should only be called during
     * testing.
     *
     * @param v The new view to install.
     */
    @Override
    public void forceNewView(CorfuDBView v) {
        this.view = v;
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
        this.view.setUUID(logID);
        this.initialView.setEpoch(epoch);
    }
}
