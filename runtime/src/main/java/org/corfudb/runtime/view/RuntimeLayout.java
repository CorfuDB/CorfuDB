package org.corfudb.runtime.view;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

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
     *
     * @throws WrongEpochException        If any server is in a higher epoch.
     * @throws QuorumUnreachableException If enough number of servers cannot be sealed.
     */
    public void moveServersToEpoch()
            throws WrongEpochException, QuorumUnreachableException {
        log.debug("Requested move of servers to new epoch {} servers are {}", layout.getEpoch(),
                layout.getAllServers());

        // Set remote epoch on all servers in layout.
        Map<String, CompletableFuture<Boolean>> resultMap =
                SealServersHelper.asyncSetRemoteEpoch(this);

        // Validate if we received enough layout server responses.
        SealServersHelper.waitForLayoutSeal(layout.getLayoutServers(), resultMap);
        // Validate if we received enough log unit server responses depending on the
        // replication mode.
        for (LayoutSegment layoutSegment : layout.getSegments()) {
            layoutSegment.getReplicationMode().validateSegmentSeal(layoutSegment, resultMap);
        }
        log.debug("Layout has been sealed successfully.");
    }


    /**
     * Sender Client Map.
     * map(client type -> map(Endpoint -> senderClient))
     * This ensures that a client for a particular endpoint stamped with the required epoch is
     * created only once.
     */
    private final Map<Class<? extends IClient>,
            Map<String, IClient>> senderClientMap = new ConcurrentHashMap<>();

    /**
     * Updates the local map of clients.
     * The epoch, client tuple is invalidated and overwritten when there is an epoch mismatch.
     * This ensures that a client for a particular endpoint stamped with the required epoch is
     * created only once.
     *
     * @param clientClass Class of client to be fetched.
     * @param endpoint    Router endpoint to create the client.
     * @return client
     */
    private IClient getClient(final Class<? extends IClient> clientClass,
                              final String endpoint) {
        return senderClientMap.compute(clientClass, (senderClass, stringEntryMap) -> {
            Map<String, IClient> endpointClientMap = stringEntryMap;
            if (endpointClientMap == null) {
                endpointClientMap = new HashMap<>();
            }

            endpointClientMap.computeIfAbsent(endpoint, s -> {
                try {
                    Constructor<? extends IClient> ctor =
                            clientClass.getDeclaredConstructor(IClientRouter.class, long.class);
                    return ctor.newInstance(getRuntime().getRouter(endpoint), layout.getEpoch());
                } catch (NoSuchMethodException | IllegalAccessException | InstantiationException
                        | InvocationTargetException e) {
                    throw new UnrecoverableCorfuError(e);
                }
            });
            return endpointClientMap;
        }).get(endpoint);
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
        return ((LogUnitClient) getClient(LogUnitClient.class, endpoint))
                .setMetricRegistry(getRuntime().getMetrics() != null
                        ? getRuntime().getMetrics() : CorfuRuntime.getDefaultMetrics())
                .setMaxWrite(getRuntime().getParameters().getMaxWriteSize());
    }

    public ManagementClient getManagementClient(String endpoint) {
        return (ManagementClient) getClient(ManagementClient.class, endpoint);
    }
}
