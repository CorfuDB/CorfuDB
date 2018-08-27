package org.corfudb.universe.cluster.vm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.messages.ContainerInfo;
import org.corfudb.universe.Universe;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.service.Service;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class VmClusterIT {
    private static final Universe UNIVERSE = Universe.getInstance();

    @Test
    public void deploySingleServiceSingleNodeTestVm() throws Exception {
        // add vm/appliances parameters
        //put services parameters into vm/appliances parameters

        CorfuServer.ServerParams serverParam = CorfuServer.ServerParams.builder()
                .mode(CorfuServer.Mode.SINGLE)
                .logDir("/tmp/log/")
                .logLevel(Level.TRACE)
                .persistence(CorfuServer.Persistence.MEMORY)
                .host("localhost")
                .port(9000)
                .build();

        Service.ServiceParams<CorfuServer.ServerParams> serviceParams = Service.ServiceParams.<CorfuServer.ServerParams>builder()
                .name("corfuServer")
                .nodes(ImmutableList.of(serverParam))
                .build();

        Cluster.ClusterParams clusterParams = Cluster.ClusterParams.builder()
                .services(ImmutableMap.of(serviceParams.getName(), serviceParams))
                .networkName("CorfuNet" + UUID.randomUUID().toString())
                .build();

        VmCluster vmCluster = UNIVERSE
                .buildVmCluster(clusterParams)
                .deploy();


        //check that cluster state is correct

    }
}