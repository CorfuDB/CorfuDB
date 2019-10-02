package org.corfudb.universe.universe.process;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.group.cluster.Cluster;
import org.corfudb.universe.group.cluster.Cluster.ClusterType;
import org.corfudb.universe.group.cluster.process.ProcessCorfuCluster;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.universe.AbstractUniverse;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.UniverseParams;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ProcessUniverse extends AbstractUniverse<NodeParams, UniverseParams> {

    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    @Builder
    public ProcessUniverse(UniverseParams universeParams) {
        super(universeParams);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    @Override
    public Universe deploy() {
        log.info("Deploy the universe: {}", universeId);

        deployGroups();

        return this;
    }

    @Override
    protected Group buildGroup(GroupParams<NodeParams> groupParams) {
        if (groupParams.getType() == ClusterType.CORFU_CLUSTER) {
            return ProcessCorfuCluster.builder()
                    .universeParams(universeParams)
                    .corfuClusterParams(ClassUtils.cast(groupParams))
                    .build();
        }

        throw new UniverseException("Unknown node type");
    }

    @Override
    public void shutdown() {
        if (destroyed.getAndSet(true)) {
            log.info("Can't shutdown `process` universe. Already destroyed");
            return;
        }

        log.info("Shutdown the universe: {}, params: {}", universeId, groups.keySet());
        shutdownGroups();
    }

    @Override
    public Universe add(GroupParams groupParams) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
