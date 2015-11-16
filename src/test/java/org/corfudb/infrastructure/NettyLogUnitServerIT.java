package org.corfudb.infrastructure;

import io.netty.util.ResourceLeakDetector;
import org.corfudb.runtime.protocols.logunits.INewWriteOnceLogUnit;
import org.corfudb.runtime.protocols.logunits.NettyLogUnitProtocol;
import org.corfudb.runtime.protocols.sequencers.NettyStreamingSequencerProtocol;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.RandomOpenPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 9/10/15.
 */
public class NettyLogUnitServerIT {

    CorfuInfrastructureBuilder infrastructure;
    NettyLogUnitProtocol proto;
    int port;
    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    //TODO: why do proto tests fail???
    @Before
    public void setup()
            throws Exception
    {
        port = RandomOpenPort.getOpenPort();
        infrastructure =
                CorfuInfrastructureBuilder.getBuilder()
                        .addLoggingUnit(port, 0, NettyLogUnitServer.class, "nlu", new HashMap<String,Object>())
                        .start(7775);
        proto =
        new NettyLogUnitProtocol("localhost", port, Collections.emptyMap(), 0);
    }

    @Test
    public void ReadOwnWrites()
            throws Exception
    {
        UUID streamID = UUID.randomUUID();
        String test = "Hello World";
        INewWriteOnceLogUnit.WriteResult wr = proto.write(0, Collections.singleton(streamID), 0, test).join();
        assertThat(wr)
                .isEqualTo(INewWriteOnceLogUnit.WriteResult.OK);
        INewWriteOnceLogUnit.ReadResult rr = proto.read(0).join();

        assertThat(rr.getPayload())
                .isEqualTo(test);

        assertThat(rr.getStreams())
                .contains(streamID);

        assertThat(rr.getStreams())
                .hasSize(1);

        assertThat(rr.getRank())
                .isEqualTo(0);
    }

    @Test
    public void OverwriteReturnsOverwrite()
            throws Exception {
        UUID streamID = UUID.randomUUID();
        String test = "Hello World";
        List<CompletableFuture<INewWriteOnceLogUnit.WriteResult>> cfList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            cfList.add(proto.write(0, Collections.singleton(streamID), 0, test));
        }
        CompletableFuture.allOf(cfList.toArray(new CompletableFuture[cfList.size()])).join();

        long okCount = cfList.stream()
                .map(CompletableFuture::join)
                .filter(wr -> wr == INewWriteOnceLogUnit.WriteResult.OK)
                .count();
        assertThat(okCount)
                .isEqualTo(1);

        long overwriteCount = cfList.stream()
                .map(CompletableFuture::join)
                .filter(wr -> wr == INewWriteOnceLogUnit.WriteResult.OVERWRITE)
                .count();
        assertThat(overwriteCount)
                .isEqualTo(999);
    }

    @Test
    public void holesAreHolesAndCannotBeOverwritten()
            throws Exception {
        UUID streamID = UUID.randomUUID();
        String test = "Hello World";
        List<CompletableFuture<INewWriteOnceLogUnit.ReadResult>> cfList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            proto.fillHole(i);
        }
        Thread.sleep(500);
        for (int i = 0; i < 1000; i++) {
            cfList.add(proto.read(0));
        }
        CompletableFuture.allOf(cfList.toArray(new CompletableFuture[cfList.size()])).join();

        long holeCount = cfList.stream()
                .map(CompletableFuture::join)
                .filter(rr -> rr.getResult() == INewWriteOnceLogUnit.ReadResultType.FILLED_HOLE)
                .count();

        assertThat(holeCount)
                .isEqualTo(1000);

        List<CompletableFuture<INewWriteOnceLogUnit.WriteResult>> cfListWrite = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            cfListWrite.add(proto.write(0, Collections.singleton(streamID), 0, test));
        }

        long overwriteCount = cfListWrite.stream()
                .map(CompletableFuture::join)
                .filter(wr -> wr == INewWriteOnceLogUnit.WriteResult.OVERWRITE)
                .count();

        assertThat(overwriteCount)
                .isEqualTo(1000);
    }



    @After
    public void tearDown()
            throws Exception
    {
        infrastructure.shutdownAndWait();
    }

}
