package org.corfudb.universe.cluster.vm;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.cluster.ClusterException;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.service.Service;

/**
 * This implementation provides a Cluster servers each of which a VM machine.
 * Deploy is going to deploy a list of servers
 */
public class VmCluster implements Cluster {

    @Override
    public VmCluster deploy() throws ClusterException {
        /**
         * setup parameters for appliances.
         * setup network?
         * start appliances
         * setup services
         * setup nodes
         * start nodes (jvm processes inside appliances). Don't need it if it's started during appliance deployment.
         */
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void shutdown() throws ClusterException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ClusterParams getClusterParams() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ImmutableList<Service> getServices() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Builder
    @Getter
    public static class VmClusterParams {
        /*
         Specify appliances parameters here
         */
    }
}
