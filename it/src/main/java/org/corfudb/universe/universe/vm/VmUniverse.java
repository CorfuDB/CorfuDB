package org.corfudb.universe.cluster.vm;

import com.google.common.collect.ImmutableMap;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.service.Group;

/**
 * This implementation provides a cluster, where each server is represented as a virtual machine.
 */
public class VmCluster implements Cluster {
    private static final UnsupportedOperationException NOT_IMPLEMETED = new UnsupportedOperationException("Not implemented");

    @Override
    public Cluster deploy() {
        throw NOT_IMPLEMETED;
    }

    @Override
    public void shutdown() {
        throw NOT_IMPLEMETED;
    }

    @Override
    public <T extends Group.GroupParams<?>> Cluster add(T serviceParams) {
        throw NOT_IMPLEMETED;
    }

    @Override
    public ClusterParams getClusterParams() {
        throw NOT_IMPLEMETED;
    }

    @Override
    public ImmutableMap<String, Group> services() {
        throw NOT_IMPLEMETED;
    }

    @Override
    public Group getService(String serviceName) {
        throw NOT_IMPLEMETED;
    }


}
