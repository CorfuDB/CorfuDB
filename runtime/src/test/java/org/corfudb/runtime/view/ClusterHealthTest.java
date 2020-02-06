package org.corfudb.runtime.view;

import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.ManagementView.ClusterHealth;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterHealthTest {
    private final LayoutUtil layoutUtil = new LayoutUtil();

    @Test
    public void testClusterHealth() {
        final String server1 = "server1";
        final String server2 = "server2";
        final String server3 = "server3";

        final List<String> servers = Arrays.asList(server1, server2, server3);
        final Layout layout = layoutUtil.getLayout(servers);
        layout.setUnresponsiveServers(Arrays.asList(server3));

        ClusterHealth clusterHealth = new ClusterHealth();
        ClusterStatus status = clusterHealth.getClusterHealth(
                layout, layout.getAllActiveServers()
        );

        assertThat(status).isEqualTo(ClusterStatus.STABLE);
    }

}
