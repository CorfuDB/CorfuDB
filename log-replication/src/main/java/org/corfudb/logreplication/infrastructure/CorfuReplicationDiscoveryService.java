package org.corfudb.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CorfuReplicationDiscoveryService implements Runnable {

    private CorfuReplicationManager replicationManager;
    private String port;

    public CorfuReplicationDiscoveryService() {
    }

    // Temp (for initial testing)
    public void setPort(String port) {
        this.port = port;
    }

    @Override
    public void run() {
        // Try to acquire the lease (Srinivas)

        // Determines current node is LEADER, then initiate Discovery Leader Receiver Protocol
            // If lease acquired, fetch from Site Manager, site information
            // ...

            // Once Site Information is retrieved, let's assume we have Site Information in the form of
            // SiteToSiteConfiguration.java
        SiteToSiteConfiguration replicationConfig = new SiteToSiteConfiguration();
        log.info("Create Log Replication Manager");
        CorfuReplicationManager replicationManager = new CorfuReplicationManager(replicationConfig);

        log.info("Start as Replicator or as Receiver, port: {}", port);
        if (replicationConfig.isLocalPrimary() && port.equals("9010")) {
                log.info("Start as Replicator");
                replicationManager.startLogReplication();
            } else {
                // Standby Site
                log.info("Start as Receiver");
                replicationManager.startLogReceive();
            }



        // Determine if current node is PRIMARY or STANDBY

        // If PRIMARY, start replication through CorfuReplicationServer.startLogReplication() ----> SourceManager.start() -> negotiation protocol
        // CorfuReplicationManager.startLogReplication();

        // If STANDBY, start SinkManager CorfuReplicationServer.startLogApply() (to receive log replicated data)
        // CorfuReplicationManager.startLogApply();

        // Probably this class can keep state and re-schedule discovery, if nothing has changed nothing is done, if
        // something changes it can stop replication...
    }
}
