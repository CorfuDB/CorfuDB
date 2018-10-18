package org.corfudb.universe.node.server;

import org.junit.Test;
import org.slf4j.event.Level;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;


public class CorfuServerParamsTest {

    @Test
    public void testEquals() {

        CorfuServerParams p1 = CorfuServerParams.serverParamsBuilder()
                .clusterName("test-cluster")
                .port(9000)
                .logLevel(Level.TRACE)
                .mode(CorfuServer.Mode.CLUSTER)
                .persistence(CorfuServer.Persistence.DISK)
                .stopTimeout(Duration.ofSeconds(123))
                .build();

        CorfuServerParams p2 = CorfuServerParams.serverParamsBuilder()
                .clusterName("test-cluster")
                .port(9000)
                .logLevel(Level.WARN)
                .mode(CorfuServer.Mode.CLUSTER)
                .persistence(CorfuServer.Persistence.DISK)
                .stopTimeout(Duration.ofSeconds(555))
                .build();

        assertThat(p1).isEqualTo(p2);
    }
}