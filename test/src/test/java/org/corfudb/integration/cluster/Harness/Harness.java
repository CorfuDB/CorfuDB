package org.corfudb.integration.cluster.Harness;

import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.view.Layout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.corfudb.AbstractCorfuTest.PARAMETERS;
import static org.corfudb.integration.AbstractIT.getCorfuServerLogPath;

/**
 * Utilities to form and manipulate a corfu cluster.
 * <p>
 * <p>Created by maithem on 7/18/18.
 */

public class Harness {

    String localAddress = "localhost";

    final int basePort = 9000;

    private String getAddressForNode(int port) {
        return "localhost" + ":" + port;
    }

    /**
     * Creates an n-node cluster layout
     * @param n number of nodes in the cluster
     * @return a layout for the cluster
     */
    private Layout getLayoutForNodes(int n) {
        List<String> layoutServers = new ArrayList<>();
        List<String> sequencer = new ArrayList<>();
        List<Layout.LayoutSegment> segments = new ArrayList<>();
        UUID clusterId = UUID.randomUUID();
        List<String> stripServers = new ArrayList<>();

        long epoch = 0;

        for (int x = 0; x < n; x++) {
            layoutServers.add(getAddressForNode(x));
            sequencer.add(getAddressForNode(x));
            stripServers.add(getAddressForNode(x));
        }

        Layout.LayoutSegment segment = new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, -1L,
                Collections.singletonList(new Layout.LayoutStripe(stripServers)));
        segments.add(segment);
        return new Layout(layoutServers, sequencer, segments, epoch, clusterId);
    }

    String getClusterConnectionString(int n) {
        String conn = "";
        for (int i = 0; i < n; i++) {
            conn += getAddressForNode(i) + ",";
        }
        return conn.substring(0, conn.length() - 1);
    }

    /**
     * Run a action(n) on nodes
     *
     * @param actions node actions
     * @throws Exception
     */
    public static void run(Action... actions) throws Exception {
        for (Action action : actions) {
            action.run();
        }
    }

    /**
     * Deploy an n-node cluster
     *
     * @param n number of nodes in a cluster
     * @return a list of provisioned nodes in the cluster
     * @throws IOException
     */
    public List<Node> deployCluster(int n) throws IOException {
        List<Node> nodes = new ArrayList<>(n);
        String conn = getClusterConnectionString(n);
        for (int i = 0; i < n; i++) {
            Process proc = new AbstractIT.CorfuServerRunner()
                    .setHost(localAddress)
                    .setPort(basePort + i)
                    .setLogPath(getCorfuServerLogPath(localAddress, basePort + i))
                    .setSingle(false)
                    .runServer();
            nodes.add(new Node(getAddressForNode(i), conn, getCorfuServerLogPath(localAddress, basePort + i)));
        }

        final Layout layout = getLayoutForNodes(n);
        final int retries = 3;
        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);
        return nodes;
    }
}
