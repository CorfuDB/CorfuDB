package org.corfudb.universe.cluster.process;

import com.google.common.collect.ImmutableMap;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.cluster.ClusterException;
import org.corfudb.universe.service.Service;

public class ProcessCluster implements Cluster {

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
    public ImmutableMap<String, Service> services() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Service getService(String serviceName) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
