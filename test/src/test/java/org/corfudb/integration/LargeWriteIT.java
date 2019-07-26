package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A set integration tests that exercise failure modes related to
 * large writes.
 */

public class LargeWriteIT  {

    @Test
    public void largeStreamWrite() throws Exception {
        String connString = "localhost:9000";
        CorfuRuntime rt = new CorfuRuntime(connString).setCacheDisabled(true).connect();

        final int totalNumWrites = 100_000 * 10;
        final int numWriters = 8;
        final int payloadSize = 1000;
        final int batchPerThread = 10;
        final int numWritesPerThread = totalNumWrites / numWriters;
        byte[] payload = new byte[payloadSize];
        Thread[] writers = new Thread[numWriters];

        long startTs = System.currentTimeMillis();

        for (int idx = 1; idx <= numWriters; idx++) {
            final int tId = idx;
            writers[idx - 1] = new Thread(() -> {
                LogUnitClient lc = rt.getLayoutView()
                        .getRuntimeLayout()
                        .getLogUnitClient(connString);
                CompletableFuture<Boolean> ft = new CompletableFuture<>();
                ft.complete(true);



                for (int x = 1; x <= numWritesPerThread; x++) {
                    LogData ld = new LogData(DataType.DATA, payload);
                    ld.useToken(new Token(0, ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)));
                    ft = lc.write(ld);
                    if (x % batchPerThread == 0) {
                        ft.join();
                    }

                }
                ft.join();

            });

            writers[idx - 1].start();
        }


        for (int x = 0; x < writers.length; x++) {
            writers[x].join();
        }

        long endTs = System.currentTimeMillis();
        System.out.println("Expected " + totalNumWrites + " found " + rt.getSequencerView().query().getToken().getSequence());
        System.out.println("throughput " + (1.0 * totalNumWrites) / (endTs - startTs));
    }
}
