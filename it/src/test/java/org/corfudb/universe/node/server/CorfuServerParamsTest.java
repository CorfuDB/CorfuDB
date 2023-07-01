package org.corfudb.universe.node.server;

import org.corfudb.universe.node.server.CorfuServerParams.ContainerResources;
import org.junit.Test;
import org.slf4j.event.Level;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;


public class CorfuServerParamsTest {

    @Test
    public void testEquals() {
        final int port = 9000;
        final int healthPort = 8080;

        CorfuServerParams p1 = CorfuServerParams.serverParamsBuilder()
                .clusterName("test-cluster")
                .port(port)
                .healthPort(healthPort)
                .logLevel(Level.TRACE)
                .mode(CorfuServer.Mode.CLUSTER)
                .persistence(CorfuServer.Persistence.DISK)
                .stopTimeout(Duration.ofSeconds(123))
                .serverVersion("1.0.0")
                .containerResources(ContainerResources.builder().build())
                .build();

        CorfuServerParams p2 = CorfuServerParams.serverParamsBuilder()
                .clusterName("test-cluster")
                .port(port)
                .healthPort(healthPort)
                .logLevel(Level.WARN)
                .mode(CorfuServer.Mode.CLUSTER)
                .persistence(CorfuServer.Persistence.DISK)
                .stopTimeout(Duration.ofSeconds(555))
                .serverVersion("1.0.0")
                .containerResources(ContainerResources.builder().build())
                .build();

        assertThat(p1).isEqualTo(p2);
    }
}