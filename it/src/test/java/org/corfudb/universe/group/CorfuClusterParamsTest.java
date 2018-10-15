package org.corfudb.universe.group;

import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuClusterParamsTest {

    @Test
    public void testFullNodeName() {
        final String clusterName = "mycluster";
        final int port = 9000;

        CorfuServerParams corfuServerParams = CorfuServerParams
                .serverParamsBuilder()
                .port(port)
                .clusterName(clusterName)
                .build();

        CorfuClusterParams clusterParams = CorfuClusterParams.builder()
                .name(clusterName)
                .nodes(Collections.singletonList(corfuServerParams))
                .build();

        String fqdn = clusterParams.getFullNodeName("node" + port);

        assertThat(fqdn).isEqualTo(clusterName + "-corfu-" + "node" + port);
    }
}