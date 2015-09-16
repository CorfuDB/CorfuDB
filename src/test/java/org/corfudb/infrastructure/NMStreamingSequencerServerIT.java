package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerMsg;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.sequencers.NettyStreamingSequencerProtocol;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CFUtils;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.corfudb.util.RandomOpenPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static io.netty.buffer.Unpooled.directBuffer;

/**
 * Created by mwei on 9/10/15.
 */
public class NMStreamingSequencerServerIT {

    CorfuInfrastructureBuilder infrastructure;

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
