package org.corfudb.universe.cluster.vm;

import com.google.common.collect.ImmutableList;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.cluster.ClusterException;
import org.corfudb.universe.service.Service;

/**
 * This implementation provides a Cluster servers each of which a VM machine.
 * Deploy is going to deploy a list of servers
 */
public class VmCluster implements Cluster {

    @Override
    public Cluster deploy() throws ClusterException {
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
}
