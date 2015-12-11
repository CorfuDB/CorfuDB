package org.corfudb.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Created by mwei on 8/26/15.
 */

@Slf4j
public class CorfuInfrastructureBuilder {

    int configMasterPort;
    Thread main;

    @SuppressWarnings("unchecked")
    public CorfuInfrastructureBuilder()
    {
    }

    /**
     * Add a sequencer to this configuration at the specified port.
     * @param port      The port this sequencer will serve on.
     * @param sequencerType       The type of sequencer to instantiate.
     * @param clientProtocol    The type of protocol to advertise to clients.
     * @param baseParams        The parameters to initialize with, or null for none.
     */
      @SneakyThrows
    @Deprecated
    @SuppressWarnings("unchecked")
    public CorfuInfrastructureBuilder addSequencer(int port, Class<?> sequencerType, String clientProtocol, Map<String,Object> baseParams)
    {
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
    @Deprecated
    @SuppressWarnings("unchecked")
    public CorfuInfrastructureBuilder addLoggingUnit(int port, int chain, Class<?> loggingType, String clientProtocol, Map<String,Object> baseParams)
    {
         return this;
    }


    /**
     * Start the configuration by initializing the configmaster at the specified port and running each server.
     * @param configMasterPort     The port to run the configuration master on.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public CorfuInfrastructureBuilder start(int configMasterPort)
    {
       this.configMasterPort = configMasterPort;
       return this;
    }

    /**
     * Factory class for getting an infrastructure builder.
     * @return  An infrastructure builder.
     */
    public static CorfuInfrastructureBuilder getBuilder()
    {
        return new CorfuInfrastructureBuilder();
    }

    /**
     * Get the configuration string for this dynamically generated instance
     * @return  A configuration string.
     */
    public String getConfigString()
    {
        return "http://localhost:" + configMasterPort + "/corfu";
    }
    /**
     * Shutdown servers and wait. 
     */
    public void shutdownAndWait()
    {
        log.info("Shutting down dynamically created infrastructure...");
    }
}
