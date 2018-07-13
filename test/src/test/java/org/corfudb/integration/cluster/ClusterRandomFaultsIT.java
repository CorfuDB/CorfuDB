package org.corfudb.integration.cluster;

import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ClusterRandomFaultsIT extends AbstractIT {

    Set<Process> serversProcs = new HashSet<>();

    String localAddress = "localhost";

    private String getAddressForNode(int port) {
        return "localhost:900" + port;
    }

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

    private List<Node> deployCluster(int n) throws IOException {
        List<Node> nodes = new ArrayList<>(n);
        String conn = getClusterConnectionString(n);
        for (int i = 0; i < n; i++) {
            Process proc = new CorfuServerRunner()
                    .setHost(localAddress)
                    .setPort(9000 + i)
                    .setLogPath(getCorfuServerLogPath(localAddress, 9000 + i))
                    .setSingle(false)
                    .runServer();
            // create n nodes
            // this is needed for clean up
            serversProcs.add(proc);
            nodes.add(new Node(getAddressForNode(i), conn, getCorfuServerLogPath(localAddress, 9000 + i)));
        }

        final Layout layout = getLayoutForNodes(n);
        final int retries = 3;
        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);
        return nodes;
    }

    // run checkpointer and free data

    // writer thread

    volatile long currentOp = 0;

    private Thread writer(Map<String, String> map, CorfuRuntime rt) {
        Runnable r = () -> {
            for (;;) {
                rt.getObjectsView().TXBegin();
                map.put(String.valueOf(currentOp), String.valueOf(currentOp));
                rt.getObjectsView().TXEnd();
                currentOp++;
            }
        };
        return new Thread(r);
    }

    private Thread statusCheck(CorfuRuntime rt) {
        Runnable r = () -> {
            long prevOp = -1;
            for (;;) {
                try {
                    rt.invalidateLayout();
                    System.out.println("current write: " + currentOp);
                    System.out.println("current tail: " + rt.getSequencerView().query());
                    System.out.println("layout: " + rt.getLayoutView().getLayout());
                    if (currentOp == prevOp) {
                        System.out.println("System stopped! ========================" );
                    }
                    prevOp = currentOp;
                    Thread.sleep(1000 * 5);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        return new Thread(r);
    }

    private void run(Action ... actions) throws Exception {
        for (Action action : actions) {
            action.run();
        }
    }

    @Test
    public void test() throws Exception {

        // disable fsync ?

        List<Node> nodes = deployCluster(3);

        Node n1 = nodes.get(0);
        Node n2 = nodes.get(1);
        Node n3 = nodes.get(2);

        CorfuRuntime rt = new CorfuRuntime(n1.getClusterAddress()).connect();

        Map<String, String> map = rt.getObjectsView().build().setStreamName("table1").setType(SMRMap.class).open();


        Thread writer = writer(map, rt);
        Thread status = statusCheck(rt);
        writer.start();
        status.start();

            Thread.sleep(1000 * 60 * 20);
            run(n1.shutdown);
            Thread.sleep(35000);
            run(n3.shutdown);
            Thread.sleep(35000);
            run(n1.start);
            Thread.sleep(10*1000);
            run(n2.shutdown);
            Thread.sleep(1000);
            run(n2.start);
        run(n1.shutdown);
        run(n1.start);


        status.join();
    }


}
