package org.corfudb.runtime.view;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This is a wrapper over the layout to provide the clients required to communicate with the nodes.
 * <p>Created by zlokhandwala on 3/8/18.
 */
@Slf4j
@Data
public class RuntimeLayout {

    @Getter
    private final Layout layout;

    /**
     * The org.corfudb.runtime this layout is associated with.
     */
    @Getter
    private final CorfuRuntime runtime;

    /**
     * Constructor taking a reference of the layout to stamp the clients.
     */
    public RuntimeLayout(@Nonnull Layout layout, @Nonnull CorfuRuntime corfuRuntime) {
        this.layout = layout;
        this.runtime = corfuRuntime;
    }

    /**
     * Attempts to move all servers in the system to the epoch of this layout.
     * The seal however waits only for a response from a quorum of layout servers (n/2 + 1),
     * a quorum of log unit servers in every stripe in case of QUORUM_REPLICATION or
     * at least one log unit server in every stripe in case of CHAIN_REPLICATION.
     * The fault detector eventually corrects out of phase epochs by resealing the servers.
     *
     * @throws WrongEpochException        If any server is in a higher epoch.
     * @throws QuorumUnreachableException If enough number of servers cannot be sealed.
     */
    public void sealMinServerSet()
            throws WrongEpochException, QuorumUnreachableException {
        log.debug("Requested move to new epoch {} servers: {}", layout.getEpoch(),
                layout.getAllServers());

        // Set remote epoch on all servers in layout.
        Map<String, CompletableFuture<Boolean>> resultMap = SealServersHelper.asyncSealServers(this);

        // Validate if we received enough layout server responses.
        SealServersHelper.waitForLayoutSeal(layout.getLayoutServers(), resultMap);
        // Validate if we received enough log unit server responses depending on the
        // replication mode.
        for (LayoutSegment layoutSegment : layout.getSegments()) {
            layoutSegment.getReplicationMode().validateSegmentSeal(layoutSegment, resultMap);
        }

        log.debug("Layout has been sealed.");
    }

    private final Map<String, Map<Integer, EndpointClients>> clientsPool = new ConcurrentHashMap<>();

    /**
     * Updates the local map of clients.
     * The epoch, client tuple is invalidated and overwritten when there is an epoch mismatch.
     * This ensures that a client for a particular endpoint stamped with the required epoch is
     * created only once.
     *
     * @param clientType Class of client to be fetched.
     * @param endpoint    Router endpoint to create the client.
     * @return client
     */

    private IClient getClient(final Class<? extends IClient> clientType,
                              final String endpoint) {

        Map<Integer, EndpointClients> endpointClients = clientsPool.computeIfAbsent(endpoint, k -> new ConcurrentHashMap<>());
        int idx = ThreadLocalRandom.current().nextInt(0, runtime.getNodeRouterPool().getMaxNumConnectionsPerNode());
        EndpointClients endpointChannel = endpointClients.computeIfAbsent(idx, k -> new EndpointClients(runtime
                .getNodeRouterPool()
                .getRouter(NodeLocator.parseString(endpoint), k)));

        if (clientType == BaseClient.class) {
            return endpointChannel.getBaseClient();
        } else if (clientType == LayoutClient.class) {
            return endpointChannel.getLayoutClient();
        }  else if (clientType == SequencerClient.class) {
            return endpointChannel.getSequencerClient();
        }  else if (clientType == LogUnitClient.class) {
            return endpointChannel.getLogUnitClient();
        }  else if (clientType == ManagementClient.class) {
            return endpointChannel.getManagementClient();
        } else {
            throw new IllegalArgumentException("unknown client type: " + clientType.getSimpleName());
        }
    }

    public BaseClient getBaseClient(String endpoint) {
        return (BaseClient) getClient(BaseClient.class, endpoint);
    }

    public LayoutClient getLayoutClient(String endpoint) {
        return (LayoutClient) getClient(LayoutClient.class, endpoint);
    }

    public SequencerClient getPrimarySequencerClient() {
        return getSequencerClient(layout.getSequencers().get(0));
    }

    public SequencerClient getSequencerClient(String endpoint) {
        return (SequencerClient) getClient(SequencerClient.class, endpoint);
    }

    public LogUnitClient getLogUnitClient(long address, int index) {
        return getLogUnitClient(layout.getStripe(address).getLogServers().get(index));
    }

    public LogUnitClient getLogUnitClient(String endpoint) {
        return ((LogUnitClient) getClient(LogUnitClient.class, endpoint));
    }

    public ManagementClient getManagementClient(String endpoint) {
        return (ManagementClient) getClient(ManagementClient.class, endpoint);
    }

    class EndpointClients {
        private final IClientRouter router;

        @Getter(lazy = true)
        private final BaseClient baseClient = new BaseClient(router, layout.getEpoch(), layout.getClusterId());

        @Getter(lazy = true)
        private final LayoutClient layoutClient = new LayoutClient(router, layout.getEpoch(), layout.getClusterId());

        @Getter(lazy = true)
        private final SequencerClient sequencerClient = new SequencerClient(router, layout.getEpoch(), layout.getClusterId());

        @Getter(lazy = true)
        private final LogUnitClient logUnitClient = new LogUnitClient(router, layout.getEpoch(), layout.getClusterId());

        @Getter(lazy = true)
        private final ManagementClient managementClient = new ManagementClient(router, layout.getEpoch(), layout.getClusterId());

        public EndpointClients(IClientRouter router) {
            this.router = router;
        }
    }
}
