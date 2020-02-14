package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.common.compression.Codec;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.corfudb.util.serializer.Serializers;
import org.corfudb.runtime.view.Stream;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A set integration tests that exercise failure modes related to
 * large writes.
 */

public class LargeWriteIT {


    @Test
    public void largeStreamWrite() throws Exception {
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();

        Stream stream = new Stream(rt, UUID.nameUUIDFromBytes("stream1".getBytes()), "s1");
        IStreamView sv = rt.getStreamsView().get(UUID.nameUUIDFromBytes("stream1".getBytes()));

        final int payloadSize = 100;
        byte[] payload = new byte[payloadSize];

        final int numWrites = 1000000;

        long ts1 = System.currentTimeMillis();
        CompletableFuture<Boolean>[] resFt = new CompletableFuture[numWrites];

        for (int x = 0; x < numWrites; x++) {
            resFt[x] = stream.asyncAppend(payload);
        }

        for (CompletableFuture<Boolean> cf : resFt) {
            cf.join();
        }

        long ts2 = System.currentTimeMillis();
        System.out.println(rt.getSequencerView().query().getToken() + " time " + (ts2 - ts1));

        //Thread.sleep(1000 * 60 * 5);
    }

}
