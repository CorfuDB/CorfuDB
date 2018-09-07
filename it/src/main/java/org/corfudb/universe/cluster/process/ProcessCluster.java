package org.corfudb.universe.cluster.process;

import com.google.common.collect.ImmutableMap;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.service.Service;

public class ProcessCluster implements Cluster {
    private static final UnsupportedOperationException NOT_IMPLEMENTED =
            new UnsupportedOperationException("Not implemented");

    @Override
    public Cluster deploy() {
        throw NOT_IMPLEMENTED;
    }

    @Override
    public void shutdown() {
        throw NOT_IMPLEMENTED;
    }

    @Override
    public ClusterParams getClusterParams() {
        throw NOT_IMPLEMENTED;
    }

    @Override
    public ImmutableMap<String, Service> services() {
        throw NOT_IMPLEMENTED;
    }

    @Override
    public Service getService(String serviceName) {
        throw NOT_IMPLEMENTED;
    }
}
