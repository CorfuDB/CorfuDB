package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ManagementViewTest {
    private final LayoutUtil layoutUtil = new LayoutUtil();
    private final ManagementView managementView = new ManagementView(Mockito.mock(CorfuRuntime.class));

    /**
     * Check that getting a quorum layout from the list of layouts works as expected
     */
    @Test
    public void testGetLayoutForQuorum() {
        final String server1 = "server1";
        final String server2 = "server2";
        final String server3 = "server3";

        final List<String> servers = Arrays.asList(server1, server2, server3);
        final Layout layout = layoutUtil.getLayout(servers);

        Map<String, Layout> layouts = new HashMap<>();

        servers.forEach(server -> layouts.put(server, layout));

        Optional<Layout> quorumLayout = managementView.getLayoutFromQuorum(layouts, layouts.size() - 1);
        assertThat(quorumLayout).isEqualTo(Optional.of(layout));

        quorumLayout = managementView.getLayoutFromQuorum(layouts, layouts.size());
        assertThat(quorumLayout).isEqualTo(Optional.of(layout));

        quorumLayout = managementView.getLayoutFromQuorum(layouts, layouts.size() + 1);
        assertThat(quorumLayout).isEqualTo(Optional.empty());
    }

    @Test
    public void testNodeStatusMap() {
        final String server1 = "server1";
        final String server2 = "server2";
        final String server3 = "server3";
        Layout layout = layoutUtil.getLayout(Arrays.asList(server1, server2, server3));
        layout.setUnresponsiveServers(Arrays.asList(server1, server2));

        Map<String, NodeStatus> status = managementView.getNodeStatusMap(layout);
        assertThat(status.get(server1)).isEqualTo(NodeStatus.DOWN);
        assertThat(status.get(server2)).isEqualTo(NodeStatus.DOWN);
        assertThat(status.get(server3)).isEqualTo(NodeStatus.UP);
    }
}
