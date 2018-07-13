package org.corfudb.integration;

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

    private String deployCluster(int n) throws IOException {
        String conn = "";
        for (int i = 0; i < n; i++) {
            // create n nodes
            serversProcs.add(new CorfuServerRunner()
                    .setHost(localAddress)
                    .setPort(9000 + i)
                    .setLogPath(getCorfuServerLogPath(localAddress, 9000 + i))
                    .setSingle(false)
                    .runServer());

            conn += getAddressForNode(i) + ",";
        }

        conn = conn.substring(0, conn.length() - 1);


        final Layout layout = getLayoutForNodes(n);
        final int retries = 3;
        BootstrapUtil.bootstrap(layout, retries, PARAMETERS.TIMEOUT_SHORT);
        return conn;
    }

    // run checkpointer and free data

    // writer thread

    volatile long currentOp = 0;

    private Thread spawnWriterThread(Map<String, String> map, CorfuRuntime rt) {
        Runnable r = () -> {
            for (; ; ) {
                rt.getObjectsView().TXBegin();
                map.put(String.valueOf(currentOp), String.valueOf(currentOp));
                rt.getObjectsView().TXEnd();
                currentOp++;
            }
        };
        return new Thread(r);
    }

    @Test
    public void test() throws Exception {

        // disable fsync ?

        String conn = deployCluster(3);

        CorfuRuntime rt = new CorfuRuntime(conn).connect();
        Map<String, String> map = rt.getObjectsView().build().setStreamName("table1").setType(SMRMap.class).open();


        Thread writer = spawnWriterThread(map, rt);
        writer.start();

        for (;;) {
            rt.invalidateLayout();
            System.out.println("current write: " + currentOp);
            System.out.println("current tail: " + rt.getSequencerView().query());
            System.out.println("layout: " + rt.getLayoutView().getLayout());
            Thread.sleep(1000*5);
        }
    }


}
