package org.corfudb.universe.group;

import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.ServerUtil;
import org.junit.Test;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuClusterParamsTest {

    @Test
    public void testFullNodeName() {
        final String clusterName = "mycluster";
        final int port = ServerUtil.getRandomOpenPort();

        CorfuServerParams param = CorfuServerParams
                .serverParamsBuilder()
                .port(port)
                .clusterName(clusterName)
                .serverVersion("1.0.0")
                .build();

        SortedSet<CorfuServerParams> corfuServers = new TreeSet<>(Collections.singletonList(param));

        CorfuClusterParams clusterParams = CorfuClusterParams.builder()
                .name(clusterName)
                .nodes(corfuServers)
                .serverVersion("1.0.0")
                .build();

        String fqdn = clusterParams.getFullNodeName("node" + port);

        assertThat(fqdn).isEqualTo(clusterName + "-corfu-" + "node" + port);
    }
}