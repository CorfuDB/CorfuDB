package org.corfudb.universe.node.server;

import org.junit.Test;
import org.slf4j.event.Level;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;


public class CorfuServerParamsTest {

    @Test
    public void testEquals() {
        final int port = 9000;

        CorfuServerParams p1 = CorfuServerParams.serverParamsBuilder()
                .clusterName("test-cluster")
                .port(port)
                .logLevel(Level.TRACE)
                .mode(CorfuServer.Mode.CLUSTER)
                .persistence(CorfuServer.Persistence.DISK)
                .stopTimeout(Duration.ofSeconds(123))
                .serverVersion("1.0.0")
                .build();

        CorfuServerParams p2 = CorfuServerParams.serverParamsBuilder()
                .clusterName("test-cluster")
                .port(port)
                .logLevel(Level.WARN)
                .mode(CorfuServer.Mode.CLUSTER)
                .persistence(CorfuServer.Persistence.DISK)
                .stopTimeout(Duration.ofSeconds(555))
                .serverVersion("1.0.0")
                .build();

        assertThat(p1).isEqualTo(p2);
    }
}