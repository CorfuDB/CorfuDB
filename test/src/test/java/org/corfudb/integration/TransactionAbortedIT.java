package org.corfudb.integration;

import org.assertj.core.api.Assertions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.corfudb.runtime.object.transactions.ConflictSetInfo;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Ensure that the aborted transaction reporting adheres to the contract.
 *
 * This test does the following:
 * 1) Creates two threads.
 * 2) Synchronizes them using a count-down latch to ensure concurrent execution.
 * 3) They both write to the same stream and the key.
 * 4) One of the threads is going to get aborted (TransactionAbortedException).
 * 5) Ensure that the exception correctly reports the offending TX ID, the stream ID and the key.
 */
public class TransactionAbortedIT extends AbstractIT {

    long offendingAddress = -1;

    private void concurrent(CorfuRuntime runtime, CountDownLatch startLatch, CountDownLatch endLatch) {

        Map<String, String> map = runtime.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(this.getClass().getSimpleName())
                .open();
        final String key = "key";
        final String value = "value";

        startLatch.countDown();
        runtime.getObjectsView().TXBegin();
        UUID myTxId = TransactionalContext.getCurrentContext().getTransactionID();
        map.put(key, value);

        try {
            startLatch.await();
        } catch (InterruptedException fail) {
            throw new RuntimeException(fail);
        }

        try {
            offendingAddress = runtime.getObjectsView().TXEnd();
        } catch (TransactionAbortedException e) {

            // Ensure that the correct stream ID is reported.
            Assertions.assertThat(e.getConflictStream()
                    .equals(CorfuRuntime.getStreamID(this.getClass().getSimpleName())));

            // Ensure that the correct offending address is reported.
            Assertions.assertThat(e.getOffendingAddress().equals(offendingAddress));

            // Ensure that the correct key is reported.
            final ICorfuSMRProxyInternal proxyInternal =
                    e.getContext().getWriteSetInfo().getConflicts().keySet().stream().findFirst().get();
            final byte[] keyHash = ConflictSetInfo.generateHashFromObject(proxyInternal, key);
            Assertions.assertThat(Arrays.equals(keyHash, e.getConflictKey())).isTrue();

            endLatch.countDown();
        }
    }

    @Test
    public void largeTransaction() throws InterruptedException, IOException {

        // Start node one and populate it with data
        Process server = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        // Configure a client with a max write limit
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder().build();

        CorfuRuntime runtime = CorfuRuntime.fromParameters(params);
        runtime.parseConfigurationString(DEFAULT_ENDPOINT);
        runtime.connect();

        CountDownLatch startLatch = new CountDownLatch(2);
        CountDownLatch endLatch = new CountDownLatch(1);

        Thread thread0 = new Thread(() -> concurrent(runtime, startLatch, endLatch));
        Thread thread1 = new Thread(() -> concurrent(runtime, startLatch, endLatch));

        thread0.start();
        thread1.start();

        thread0.join();
        thread1.join();

        Assertions.assertThat(endLatch.getCount() == 0)
                .withFailMessage("There was a problem with TransactionAborted exception reporting.")
                .isTrue();
        shutdownCorfuServer(server);
    }
}
