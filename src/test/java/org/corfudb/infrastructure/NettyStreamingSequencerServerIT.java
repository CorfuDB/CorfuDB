package org.corfudb.infrastructure;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.protocols.sequencers.NettyStreamingSequencerProtocol;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.corfudb.util.RandomOpenPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.*;

import static io.netty.buffer.Unpooled.directBuffer;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 9/10/15.
 */
public class NettyStreamingSequencerServerIT {

    CorfuInfrastructureBuilder infrastructure;
    NettyStreamingSequencerProtocol proto;
    int port;
    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    @Before
    public void setup()
            throws Exception
    {
        port = RandomOpenPort.getOpenPort();
        infrastructure =
                CorfuInfrastructureBuilder.getBuilder()
                        .addSequencer(port, NettyStreamingSequencerServer.class, "nsss", null)
                        .start(7775);
        proto =
        new NettyStreamingSequencerProtocol("localhost", port, Collections.emptyMap(), 0);
    }

    @Test
    public void sequenceNumbersAreIncreasing()
    throws Exception
    {
        long lastSequence = proto.getNext(Collections.singleton(UUID.randomUUID()), 1).get();
        for (int i = 0; i < 100; i++)
        {
            long thisSequence = proto.getNext(Collections.singleton(UUID.randomUUID()), 1).get();
            assertThat(thisSequence)
                    .isGreaterThan(lastSequence);
            lastSequence  = thisSequence;
        }
    }

    //@Test
    public void perStreamSequenceNumbersWork()
            throws Exception
    {
        UUID firstStream = UUID.randomUUID();
        long firstSequence = proto.getNext(Collections.singleton(firstStream), 1).get();
        for (int i = 0; i < 100; i++)
        {
            proto.getNext(Collections.singleton(UUID.randomUUID()), 1).get();
        }
        long lastSequence = proto.getNext(Collections.singleton(firstStream), 0).get();
        assertThat(firstSequence)
                .isEqualTo(lastSequence);
    }

    @Test
    public void mtTest()
    {
        try {
            NettyStreamingSequencerProtocol proto =
                    new NettyStreamingSequencerProtocol("localhost", port, Collections.emptyMap(), 0);
            System.out.println("next: " + proto.getNext(Collections.singleton(UUID.randomUUID()), 1).get());
            System.out.println("next: " + proto.getNext(Collections.singleton(UUID.randomUUID()), 1).get());
            ICorfuDBInstance instance = CorfuDBRuntime.getRuntime(infrastructure.getConfigString()).getLocalInstance();
            instance.getStreamingSequencer().getNext(UUID.randomUUID(), 1);
            proto.getNext(Collections.singleton(UUID.randomUUID()), 1);
            ExecutorService es = Executors.newFixedThreadPool(4);
            Runnable r = () -> {
            CompletableFuture<Long> cf1 = proto.getNext(Collections.singleton(UUID.randomUUID()), 1);
            CompletableFuture<Long> cf2 = proto.getNext(Collections.singleton(UUID.randomUUID()), 1);
            CompletableFuture<Long> cf3 = proto.getNext(Collections.singleton(UUID.randomUUID()), 1);
                try {
                    System.out.println("cf1: " + cf1.get());
                    System.out.println("cf2: " + cf2.get());
                    System.out.println("cf3: " + cf3.get());
                } catch (InterruptedException | ExecutionException e ) {
                    e.printStackTrace();
                }
            };
            es.submit(r);
            es.submit(r);
            es.submit(r);
            es.shutdown();

        } catch (Exception ne)
        {
            ne.printStackTrace();
        }
    }

    @After
    public void tearDown()
            throws Exception
    {
        infrastructure.shutdownAndWait();
    }

}
