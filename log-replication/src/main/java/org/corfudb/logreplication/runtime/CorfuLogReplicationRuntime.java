package org.corfudb.logreplication.runtime;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.cluster.ClusterDescriptor;
import org.corfudb.transport.logreplication.LogReplicationClientRouter;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.logreplication.LogReplicationSourceManager;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Client Runtime to connect to a Corfu Log Replication Server.
 *
 * The Log Replication Server is currently conformed uniquely by
 * a Log Replication Unit. This client runtime is a wrapper around all
 * units.
 *
 *
 * @author amartinezman
 */
@Slf4j
public class CorfuLogReplicationRuntime {
    /**
     * The parameters used to configure this {@link CorfuLogReplicationRuntime}.
     */
    @Getter
    private final LogReplicationRuntimeParameters parameters;

    /**
     * Log Replication Client Router
     */
    private LogReplicationClientRouter router;

    /**
     * Log Replication Client
     */
    private LogReplicationClient client;

    /**
     * Log Replication Source Manager (producer of log updates)
     */
    @Getter
    private LogReplicationSourceManager sourceManager;

    /**
     * Log Replication Configuration
     */
    private LogReplicationConfig logReplicationConfig;

    /**
     * Constructor
     *
     * @param parameters runtime parameters
     */
    public CorfuLogReplicationRuntime(@Nonnull LogReplicationRuntimeParameters parameters) {
        this.parameters = parameters;
        this.logReplicationConfig = parameters.getReplicationConfig();
    }

    /**
     * Set the transport layer for communication to remote cluster.
     *
     * The transport layer encompasses:
     *
     * - Clients to direct remote counter parts (handles specific funcionalities)
     * - A router which will manage different clients (currently
     * we only communicate to the Log Replication Server)
     */
    private void configureTransport(CorfuReplicationDiscoveryService discoveryService) {

        ClusterDescriptor remoteSite = parameters.getRemoteClusterDescriptor();
        String remoteClusterId = remoteSite.getClusterId();

        log.debug("Configure transport to remote cluster {}", remoteClusterId);

        // This Router is more of a forwarder (and client manager), as it is not specific to a remote node,
        // but instead valid to all nodes in a remote cluster. Routing to a specific node (endpoint)
        // is the responsibility of the transport layer in place (through the adapter).
        router = new LogReplicationClientRouter(remoteSite, getParameters());
        router.addClient(new LogReplicationHandler());
        router.start();
        // Instantiate Clients to remote server (Log Replication only supports one server for now, Log Replication Unit)
        client = new LogReplicationClient(router, parameters.getRemoteClusterDescriptor(), discoveryService);
    }

    public void connect(CorfuReplicationDiscoveryService discoveryService) {
        configureTransport(discoveryService);
        log.info("Connected. Set Source Manager to connect to local Corfu on {}", parameters.getLocalCorfuEndpoint());
        sourceManager = new LogReplicationSourceManager(parameters.getLocalCorfuEndpoint(),
                    client, logReplicationConfig);
    }

    public LogReplicationQueryLeaderShipResponse queryLeadership() throws ExecutionException, InterruptedException {
        log.info("Request leadership of client {}:{}", client.getRouter().getHost(), client.getRouter().getPort());
        return client.sendQueryLeadership().get();
    }

    public LogReplicationNegotiationResponse startNegotiation() throws Exception {
        log.info("Send Negotiation Request");
        return client.sendNegotiationRequest().get();
    }

    /**
     * Start snapshot (full) sync
     */
    public void startSnapshotSync() {
        UUID snapshotSyncRequestId = sourceManager.startSnapshotSync();
        log.info("Start Snapshot Sync[{}]", snapshotSyncRequestId);
    }

    public void startReplication(LogReplicationEvent event) {
        sourceManager.startReplication(event);
    }

    /**
     * Start log entry (delta) sync
     */
    public void startLogEntrySync(LogReplicationEvent event) {
        log.info("Start Log Entry Sync");
        sourceManager.startReplication(event);
    }

    /**
     * Clean up router, stop source manager.
     */
    //TODO: stop the router etc.
    public void stop() {
        sourceManager.shutdown();
    }

    /**
     *
     *
     * @param ts
     * @return
     */
    public long getNumEntriesToSend(long ts) {
        long ackTS = sourceManager.getLogReplicationFSM().getAckedTimestamp();
        return ts - ackTS;
    }

}
