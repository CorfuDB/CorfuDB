package org.corfudb.infrastructure;

import io.netty.util.ResourceLeakDetector;
import org.corfudb.runtime.CorfuDBRuntimeIT;
import org.corfudb.runtime.protocols.logunits.INewWriteOnceLogUnit;
import org.corfudb.runtime.protocols.logunits.NettyLogUnitProtocol;
import org.corfudb.runtime.protocols.sequencers.NettyStreamingSequencerProtocol;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.IStreamAddressSpace;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;
import org.corfudb.runtime.view.WriteOnceAddressSpace;
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

    IStreamAddressSpace proto;
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
        proto = CorfuDBRuntimeIT.generateInstance().getStreamAddressSpace();
    }

    @Test
    public void ReadOwnWrites()
            throws Exception
    {
        UUID streamID = UUID.randomUUID();
        String test = "Hello World";
        IStreamAddressSpace.StreamAddressWriteResult wr = proto.writeAsync(0, Collections.singleton(streamID), test).join();
        assertThat(wr)
                .isEqualTo(INewWriteOnceLogUnit.WriteResult.OK);
        IStreamAddressSpace.StreamAddressSpaceEntry rr = proto.readAsync(0).join();

        assertThat(rr.getPayload())
                .isEqualTo(test);

        assertThat(rr.getStreams())
                .contains(streamID);

        assertThat(rr.getStreams())
                .hasSize(1);
    }

    @Test
    public void OverwriteReturnsOverwrite()
            throws Exception {
        UUID streamID = UUID.randomUUID();
        String test = "Hello World";
        List<CompletableFuture<IStreamAddressSpace.StreamAddressWriteResult>> cfList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            cfList.add(proto.writeAsync(0, Collections.singleton(streamID), test));
        }
        CompletableFuture.allOf(cfList.toArray(new CompletableFuture[cfList.size()])).join();

        long okCount = cfList.stream()
                .map(CompletableFuture::join)
                .filter(wr -> wr == IStreamAddressSpace.StreamAddressWriteResult.OK)
                .count();
        assertThat(okCount)
                .isEqualTo(1);

        long overwriteCount = cfList.stream()
                .map(CompletableFuture::join)
                .filter(wr -> wr == IStreamAddressSpace.StreamAddressWriteResult.OVERWRITE)
                .count();
        assertThat(overwriteCount)
                .isEqualTo(999);
    }

    @Test
    public void holesAreHolesAndCannotBeOverwritten()
            throws Exception {
        UUID streamID = UUID.randomUUID();
        String test = "Hello World";
        List<CompletableFuture<IStreamAddressSpace.StreamAddressSpaceEntry>> cfList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            proto.fillHole(i);
        }
        Thread.sleep(500);
        for (int i = 0; i < 1000; i++) {
            cfList.add(proto.readAsync(0));
        }
        CompletableFuture.allOf(cfList.toArray(new CompletableFuture[cfList.size()])).join();

        long holeCount = cfList.stream()
                .map(CompletableFuture::join)
                .filter(rr -> rr.getCode() == IStreamAddressSpace.StreamAddressEntryCode.HOLE)
                .count();

        assertThat(holeCount)
                .isEqualTo(1000);

        List<CompletableFuture<IStreamAddressSpace.StreamAddressWriteResult>> cfListWrite = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            cfListWrite.add(proto.writeAsync(0, Collections.singleton(streamID), test));
        }

        long overwriteCount = cfListWrite.stream()
                .map(CompletableFuture::join)
                .filter(wr -> wr == IStreamAddressSpace.StreamAddressWriteResult.OK)
                .count();

        assertThat(overwriteCount)
                .isEqualTo(1000);
    }
}
