package org.corfudb.logreplication.infrastructure;

public class CorfuReplicationDiscoveryService implements Runnable {

    private final CorfuReplicationServerNode logReplicationServer;

    public CorfuReplicationDiscoveryService(CorfuReplicationServerNode logReplicationServer) {
        this.logReplicationServer = logReplicationServer;
    }

    @Override
    public void run() {
        // Try to acquire the lease (Srinivas)

        // Determines current node is LEADER, then initiate Discovery Leader Receiver Protocol
            // If lease acquired, fetch from Site Manager, site information
            // ...

        // Determine if current node is PRIMARY or STANDBY

        // If PRIMARY, start replication through CorfuReplicationServer.startLogReplication() ----> SourceManager.start() -> negotiation protocol

        // If STANDBY, start SinkManager CorfuReplicationServer.startLogApply() (to receive log replicated data)

    }
}
