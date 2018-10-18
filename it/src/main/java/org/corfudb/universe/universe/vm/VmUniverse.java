package org.corfudb.universe.universe.vm;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.group.cluster.vm.VmCorfuCluster;
import org.corfudb.universe.universe.AbstractUniverse;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.util.ClassUtils;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Represents VM implementation of a {@link Universe}.
 * <p>
 * The following are the main functionalities provided by this class:
 * </p>
 * DEPLOY: first deploys VMs on vSphere (if not exist), then deploys the group (corfu server) on the VMs
 * SHUTDOWN: stops the {@link Universe}, i.e. stops the existing {@link Group} gracefully within the provided timeout
 */
@Slf4j
public class VmUniverse extends AbstractUniverse<VmUniverseParams> {
    private final AtomicBoolean destroyed = new AtomicBoolean(false);
    @NonNull
    private final ApplianceManager applianceManager;

    @Builder
    public VmUniverse(VmUniverseParams universeParams, ApplianceManager applianceManager) {
        super(universeParams);
        this.applianceManager = applianceManager;
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    /**
     * Deploy a vm specific {@link Universe} according to provided parameter, vSphere APIs, and other components.
     *
     * @return Current instance of a VM {@link Universe} would be returned.
     * @throws UniverseException this exception will be thrown if deploying a {@link Universe} is not successful
     */
    @Override
    public VmUniverse deploy() {
        log.info("Deploy the universe: {}", universeId);

        applianceManager.deploy();
        deployGroups();

        return this;
    }

    /**
     * Deploy a {@link Group} on existing VMs according to input parameter.
     */
    @Override
    protected Group buildGroup(GroupParams groupParams) {
        switch (groupParams.getNodeType()) {
            case CORFU_SERVER:
                return VmCorfuCluster.builder()
                        .universeParams(universeParams)
                        .params(ClassUtils.cast(groupParams))
                        .vms(applianceManager.getVms())
                        .build();
            case CORFU_CLIENT:
                throw new UniverseException("Not implemented corfu client. Group config: " + groupParams);
            default:
                throw new UniverseException("Unknown node type");
        }
    }

    /**
     * Shutdown the {@link Universe} by stopping each of its {@link Group}.
     */
    @Override
    public void shutdown() {
        if (destroyed.getAndSet(true)) {
            log.info("Can't shutdown vm universe. Already destroyed");
            return;
        }

        log.info("Shutdown the universe: {}, params: {}", universeId, groups);
        shutdownGroups();
    }

    @Override
    public Universe add(GroupParams groupParams) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
