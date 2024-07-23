package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.replication.send.SenderBufferManager;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

public class SenderBufferManagerTest {
    @Mock
    private DataSender mockDataSender;

    private TestSenderBufferManager bufferManager;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        bufferManager = new TestSenderBufferManager(mockDataSender);
    }

    /*
     * Test that log entry CFs that timeout due to no ACK from the sink trigger a
     * backoff wait, that is disabled upon a successful ack within the allotted timeframe.
     */
    @Test
    public void testSenderBackpressure() {
        LogReplicationEntryMetadataMsg testMsgMetadata1 = LogReplicationEntryMetadataMsg.newBuilder()
                .setTimestamp(1L).build();
        LogReplicationEntryMetadataMsg testMsgMetadata2 = LogReplicationEntryMetadataMsg.newBuilder()
                .setTimestamp(2L).build();
        LogReplicationEntryMsg testMsg1 = LogReplicationEntryMsg.newBuilder()
                .setMetadata(testMsgMetadata1).build();
        LogReplicationEntryMsg testMsg2 = LogReplicationEntryMsg.newBuilder()
                .setMetadata(testMsgMetadata2).build();

        when(mockDataSender.send(any(LogReplicationEntryMsg.class)))
                .thenReturn(CompletableFuture.completedFuture(testMsg1))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("Future Timed Out!")))
                .thenReturn(CompletableFuture.completedFuture(testMsg2));

        // Message 1 goes through and receives ack
        LogReplicationEntryMsg ack1 = bufferManager.resend(true);
        assertNull(ack1, "No stale ACKs to process!");
        bufferManager.sendWithBuffering(testMsg1);
        assertFalse(bufferManager.isBackpressureActive());

        // Message 2 is sent
        LogReplicationEntryMsg ack2 = bufferManager.resend(true);
        assertEquals(ack2, testMsg1, "Message 1 had been ACKd by the sink!");
        bufferManager.sendWithBuffering(testMsg2);
        assertFalse(bufferManager.isBackpressureActive());

        // Message 2 times out when processing further!
        LogReplicationEntryMsg ack3 = bufferManager.resend(true);
        assertNull(ack3, "Message 2 timed out giving us a null ack!");
        assertTrue(bufferManager.isBackpressureActive());
        bufferManager.sendWithBuffering(testMsg2);

        // Next go around everything is fine and backpressure should be turned off
        bufferManager.resend(true);
        assertFalse(bufferManager.isBackpressureActive());
    }

    /*
     * Test that back pressure wait time increases on repeat timeouts.
     */
    @Test
    public void testSenderBackpressureWaitIncrease() {
        LogReplicationEntryMetadataMsg testMsgMetadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setTimestamp(1L).build();
        LogReplicationEntryMsg testMsg = LogReplicationEntryMsg.newBuilder()
                .setMetadata(testMsgMetadata).build();

        when(mockDataSender.send(any(LogReplicationEntryMsg.class)))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("Future Timed Out!")))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("Future Timed Out!")))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("Future Timed Out!")))
                .thenReturn(CompletableFuture.completedFuture(testMsg));

        // Initial attempt, back pressure not activated, and sleep time is at initial
        bufferManager.resend(true);
        bufferManager.sendWithBuffering(testMsg);
        assertEquals(SenderBufferManager.INITIAL_SLEEP_TIME_MS, bufferManager.getBackoffRetry().getCurrentSleepTime());

        // Backpressure is activated, and sleep time is at initial
        bufferManager.resend(true);
        bufferManager.sendWithBuffering(testMsg);
        assertTrue(bufferManager.isBackpressureActive());
        assertEquals(SenderBufferManager.INITIAL_SLEEP_TIME_MS,
                bufferManager.getBackoffRetry().getCurrentSleepTime());

        // Backpressure is increased, and sleep time is at initial + increment
        bufferManager.resend(true);
        bufferManager.sendWithBuffering(testMsg);
        assertEquals(SenderBufferManager.INITIAL_SLEEP_TIME_MS + SenderBufferManager.SLEEP_TIME_INCREMENT_MS,
                bufferManager.getBackoffRetry().getCurrentSleepTime());

        // Backpressure is reset, and sleep time is at initial wait
        bufferManager.resend(true);
        bufferManager.sendWithBuffering(testMsg);
        assertFalse(bufferManager.isBackpressureActive());
        assertEquals(SenderBufferManager.INITIAL_SLEEP_TIME_MS, bufferManager.getBackoffRetry().getCurrentSleepTime());
    }

    /**
     * Test implementation of SenderBufferManager
     *
     */
    private static class TestSenderBufferManager extends SenderBufferManager {

        public TestSenderBufferManager(DataSender dataSender) {
            super(dataSender);
        }

        @Override
        public void addCFToAcked(LogReplicationEntryMsg message, CompletableFuture<LogReplicationEntryMsg> cf) {
            getPendingCompletableFutureForAcks().put(message.getMetadata().getTimestamp(), cf);
        }

        @Override
        public void updateAck(Long newAck) {

        }

        @Override
        public void updateAck(LogReplicationEntryMsg entry) {
            getPendingCompletableFutureForAcks().remove(entry.getMetadata().getTimestamp());
        }
    }
}
