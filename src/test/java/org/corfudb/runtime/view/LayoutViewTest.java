package org.corfudb.runtime.view;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/6/16.
 */
public class LayoutViewTest extends AbstractViewTest {
    @Test
    public void canGetLayout() {
        CorfuRuntime r = getDefaultRuntime().connect();
        Layout l = r.getLayoutView().getCurrentLayout();
        assertThat(l.asJSONString())
                .isNotNull();
    }

    @Test
    public void canSetLayout()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime().connect();
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addSequencer(9000)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addToSegment()
                .addToLayout()
                .build();
        l.setRuntime(r);
        r.getLayoutView().updateLayout(l, 1L);
        r.invalidateLayout();
        assertThat(r.getLayoutView().getLayout().epoch)
                .isEqualTo(1L);
    }

    @Test
    public void canTolerateLayoutServerFailure()
            throws Exception {
        addServer(9000);
        addServer(9001);

        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addSequencer(9000)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addToSegment()
                .addToLayout()
                .build());

        CorfuRuntime r = getRuntime().connect();

        // Fail the network link between the client and test server
        addServerRule(9001, new TestRule()
                .always()
                .drop());

        r.invalidateLayout();

        r.getStreamsView().get(CorfuRuntime.getStreamID("hi")).check();
    }

    /**
     * Failing 2 nodes having:
     * 9000 => Layout server, sequencer server and logunit server
     * 9004 => Layout server and logunit server
     * Details:
     * Start with a configuration of 5 servers 9000, 9001, 9002, 9003 and 9004.
     * Query the layout. Fail 9000 and 9004 and wait for management server to detect failures.
     *
     * @throws Exception
     */
    @Test
    public void handleMultipleServerFailure()
            throws Exception {
        addServer(9000);
        addServer(9001);
        addServer(9002);
        addServer(9003);
        addServer(9004);
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addLayoutServer(9002)
                .addLayoutServer(9003)
                .addLayoutServer(9004)
                .addSequencer(9000)
                .addSequencer(9002)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addLogUnit(9002)
                .addLogUnit(9003)
                .addLogUnit(9004)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();

        // All three layout servers 9000, 9001 and 9002 present
        Layout layout = corfuRuntime.getLayoutView().getLayout();
        assertThat(layout.getLayoutServers().size()).isEqualTo(5);
        assertThat(layout.getSequencers().size()).isEqualTo(2);
        Layout.LayoutSegment layoutSegment = layout.getSegments().get(0);
        assertThat(layoutSegment.getStripes().get(0).getLogServers().size()).isEqualTo(4);

        // Adding a rule on 9000 to drop all packets
        addServerRule(9000, new TestRule().always().drop());
        addServerRule(9004, new TestRule().always().drop());
        // Waiting for failure detectors to detect and correct the layout. At least 3 polls(1 second each)
        // Give more time for runtime to make retries if it tries to fetch layout from a failed node.
        Thread.sleep(10000);

        corfuRuntime.invalidateLayout();

        layout = corfuRuntime.getLayoutView().getLayout();
        assertThat(layout.getLayoutServers().size()).isEqualTo(3);
        assertThat(layout.getSequencers().size()).isEqualTo(1);
        layoutSegment = layout.getSegments().get(0);
        assertThat(layoutSegment.getStripes().get(0).getLogServers().size()).isEqualTo(2);

    }

    /**
     * Fail the server with primary sequencer and reconfigure
     * while data operations are going on.
     * Details:
     * Start with a configuration of 3 servers 9000, 9001, 9002.
     * Perform data operations. Fail 9000 and reconfigure to have only 9001 and 9002.
     * Perform data operations while the reconfiguration is going on. The operations should
     * be stuck till the new configuration is chosen and then complete after that.
     *
     * Currently facing a race condition due to layout mismatch. Working to pass this test.
     *
     * @throws Exception
     */
//    @Test
//    public void reconfigurationDuringDataOperations()
//            throws Exception {
//        addServer(9000);
//        addServer(9001);
//        addServer(9002);
//        Layout l = new TestLayoutBuilder()
//                .setEpoch(1L)
//                .addLayoutServer(9000)
//                .addLayoutServer(9001)
//                .addLayoutServer(9002)
//                .addSequencer(9000)
//                .addSequencer(9001)
//                .addSequencer(9002)
//                .buildSegment()
//                .buildStripe()
//                .addLogUnit(9000)
//                .addLogUnit(9001)
//                .addLogUnit(9002)
//                .addToSegment()
//                .addToLayout()
//                .build();
//        bootstrapAllServers(l);
//        CorfuRuntime corfuRuntime = getRuntime(l).connect();
//
//        // Thread to reconfigure the layout
//        CountDownLatch layoutReconfiguredLatch = new CountDownLatch(1);
//
//        Thread t = new Thread(() -> {
//            try {
//                corfuRuntime.invalidateLayout();
//
//                // Fail the network link between the client and test server
//                addServerRule(9002, new TestRule().always().drop());
//
//                System.out.println("After updateLayout ");
//                layoutReconfiguredLatch.countDown();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//        t.start();
//
//        // verify writes and reads happen before and after the reconfiguration
//        StreamView sv = corfuRuntime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
//        // the write might happen before the reconfiguration while the read for that write
//        // will happen after reconfiguration
//        writeAndReadStream(sv, layoutReconfiguredLatch);
//        // Write and read after reconfiguration.
//        writeAndReadStream(sv, layoutReconfiguredLatch);
//        t.join();
//    }
//
//    private void writeAndReadStream(StreamView sv, CountDownLatch latch) throws InterruptedException {
//        byte[] testPayload = "hello world".getBytes();
//        sv.write(testPayload);
//        latch.await();
//        assertThat(sv.read().getPayload())
//                .isEqualTo("hello world".getBytes());
//        assertThat(sv.read()).isEqualTo(null);
//    }

}