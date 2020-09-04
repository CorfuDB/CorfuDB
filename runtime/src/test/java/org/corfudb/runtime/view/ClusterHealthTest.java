package org.corfudb.runtime.view;

import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.ManagementView.ClusterHealth;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterHealthTest {
    private final LayoutUtil layoutUtil = new LayoutUtil();

    private final String server1 = "server1";
    private final String server2 = "server2";
    private final String server3 = "server3";
    private final List<String> servers = Arrays.asList(server1, server2, server3);

    private final ClusterHealth clusterHealth = new ClusterHealth();

    /**
     * Test possible cluster statuses for layout servers
     */
    @Test
    public void testLayoutServersHealth(){
        Layout layout = layoutUtil.getLayout(servers);
        ClusterStatus status = clusterHealth.getLayoutServersClusterHealth(
                layout, layout.getAllActiveServers()
        );
        assertThat(status).isEqualTo(ClusterStatus.STABLE);

        layout.setUnresponsiveServers(Collections.singletonList(server3));
        status = clusterHealth.getLayoutServersClusterHealth(
                layout, layout.getAllActiveServers()
        );
        assertThat(status).isEqualTo(ClusterStatus.DEGRADED);

        layout.setUnresponsiveServers(Arrays.asList(server2, server3));
        status = clusterHealth.getLayoutServersClusterHealth(
                layout, layout.getAllActiveServers()
        );
        assertThat(status).isEqualTo(ClusterStatus.UNAVAILABLE);
    }

    /**
     * Test possible cluster statuses for sequencer servers
     */
    @Test
    public void testSequencerServersHealth(){
        Layout layout = layoutUtil.getLayout(servers);
        layout.setUnresponsiveServers(Collections.singletonList(server3));

        ClusterStatus status = clusterHealth.getSequencerServersClusterHealth(
                layout, layout.getAllActiveServers()
        );
        assertThat(status).isEqualTo(ClusterStatus.STABLE);

        //Unresponsive sequencer
        layout.setUnresponsiveServers(Collections.singletonList(server1));
        status = clusterHealth.getSequencerServersClusterHealth(
                layout, layout.getAllActiveServers()
        );
        assertThat(status).isEqualTo(ClusterStatus.UNAVAILABLE);
    }

    /**
     * Test possible cluster statuses for LogUnit servers
     */
    @Test
    public void testLogUnitServersClusterHealth(){
        Layout layout = layoutUtil.getLayout(servers);

        ClusterStatus status = clusterHealth.getLogUnitServersClusterHealth(
                layout, layout.getAllActiveServers()
        );
        assertThat(status).isEqualTo(ClusterStatus.STABLE);

        //invalid segment
        layout.setUnresponsiveServers(Collections.singletonList(server3));
        status = clusterHealth.getLogUnitServersClusterHealth(
                layout, layout.getAllActiveServers()
        );
        assertThat(status).isEqualTo(ClusterStatus.UNAVAILABLE);

        //exclude unresponsive server
        layout.getFirstSegment().getFirstStripe().getLogServers().remove(server3);
        status = clusterHealth.getLogUnitServersClusterHealth(
                layout, layout.getAllActiveServers()
        );
        assertThat(status).isEqualTo(ClusterStatus.STABLE);
    }

    /**
     * Get cluster status from a layout with all responsive nodes.
     * Expected result is STABLE status
     */
    @Test
    public void testClusterHealth() {
        Layout layout = layoutUtil.getLayout(servers);
        layout.setUnresponsiveServers(Collections.singletonList(server3));

        //invalid log unit state
        ClusterStatus status = clusterHealth.getClusterHealth(
                layout, layout.getAllActiveServers()
        );
        assertThat(status).isEqualTo(ClusterStatus.UNAVAILABLE);

        //stable state with an unresponsive server
        layout.getFirstSegment().getFirstStripe().getLogServers().remove(server3);
        status = clusterHealth.getClusterHealth(
                layout, layout.getAllActiveServers()
        );
        assertThat(status).isEqualTo(ClusterStatus.DEGRADED);
    }
}
