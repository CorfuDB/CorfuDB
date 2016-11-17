package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/6/16.
 */
@Slf4j
public class LayoutViewTest extends AbstractViewTest {
    //@Test
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
                .setEpoch(1)
                .addLayoutServer(9000)
                .addSequencer(9000)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addToSegment()
                .addToLayout()
                .build();
        l.setRuntime(r);
        l.moveServersToEpoch();
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
     * Fail a server and reconfigure
     * while data operations are going on.
     * Details:
     * Start with a configuration of 3 servers 9000, 9001, 9002.
     * Perform data operations. Fail 9001 and reconfigure to have only 9000 and 9002.
     * Perform data operations while the reconfiguration is going on. The operations should
     * be stuck till the new configuration is chosen and then complete after that.
     * FIXME: We cannot failover the server with the primary sequencer yet.
     *
     * @throws Exception
     */
    @Test
    public void reconfigurationDuringDataOperations()
            throws Exception {
        addServer(9000);
        addServer(9001);
        addServer(9002);
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addLayoutServer(9002)
                .addSequencer(9000)
                .addSequencer(9001)
                .addSequencer(9002)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addLogUnit(9001)
                .addLogUnit(9002)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();

        // Thread to reconfigure the layout
        CountDownLatch startReconfigurationLatch = new CountDownLatch(1);
        CountDownLatch layoutReconfiguredLatch = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                startReconfigurationLatch.await();
                corfuRuntime.invalidateLayout();

                // Fail the network link between the client and test server
                addServerRule(9001, new TestRule().always().drop());
                // New layout removes the failed server 9000
                Layout newLayout = new TestLayoutBuilder()
                        .setEpoch(l.getEpoch() + 1)
                        .addLayoutServer(9000)
                        .addLayoutServer(9002)
                        .addSequencer(9000)
                        .addSequencer(9002)
                        .buildSegment()
                        .buildStripe()
                        .addLogUnit(9000)
                        .addLogUnit(9002)
                        .addToSegment()
                        .addToLayout()
                        .build();
                newLayout.setRuntime(corfuRuntime);
                //TODO need to figure out if we can move to
                //update layout
                newLayout.moveServersToEpoch();

                corfuRuntime.getLayoutView().updateLayout(newLayout, newLayout.getEpoch());
                corfuRuntime.invalidateLayout();
                log.debug("layout updated new layout {}", corfuRuntime.getLayoutView().getLayout());
                layoutReconfiguredLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t.start();

        // verify writes and reads happen before and after the reconfiguration
        StreamView sv = corfuRuntime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        // This write will happen before the reconfiguration while the read for this write
        // will happen after reconfiguration
        writeAndReadStream(corfuRuntime, sv, startReconfigurationLatch, layoutReconfiguredLatch);
        // Write and read after reconfiguration.
        writeAndReadStream(corfuRuntime, sv, startReconfigurationLatch, layoutReconfiguredLatch);
        t.join();
    }

    private void writeAndReadStream(CorfuRuntime corfuRuntime, StreamView sv, CountDownLatch startReconfigurationLatch, CountDownLatch layoutReconfiguredLatch) throws InterruptedException {
        byte[] testPayload = "hello world".getBytes();
        sv.write(testPayload);
        startReconfigurationLatch.countDown();
        layoutReconfiguredLatch.await();
        assertThat(sv.read().getPayload(corfuRuntime)).isEqualTo("hello world".getBytes());
        assertThat(sv.read()).isEqualTo(null);
    }

}
