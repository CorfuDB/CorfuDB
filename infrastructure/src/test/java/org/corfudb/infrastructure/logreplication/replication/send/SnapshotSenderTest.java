package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_TIMEOUT_MS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the busy-signal-aware ack wait introduced to break the snapshot-sync ack-timeout
 * livelock: a source that receives no ack for the final snapshot-sync message no longer gives up
 * immediately -- it polls the sink directly, and only treats the sink as stalled once it has been
 * silent (no ack, and no "still processing" response to the poll) for
 * SNAPSHOT_SYNC_ACK_MAX_RETRIES consecutive attempts.
 */
@Slf4j
public class SnapshotSenderTest {

    private DataSender dataSender;
    private SnapshotSender snapshotSender;

    @Before
    public void setup() {
        LogReplicationFSM fsm = mock(LogReplicationFSM.class);
        LogReplicationAckReader ackReader = mock(LogReplicationAckReader.class);
        when(fsm.getAckReader()).thenReturn(ackReader);

        dataSender = mock(DataSender.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        SnapshotReader snapshotReader = mock(SnapshotReader.class);
        ReadProcessor readProcessor = mock(ReadProcessor.class);

        snapshotSender = new SnapshotSender(runtime, snapshotReader, dataSender, readProcessor, 5, fsm);
    }

    private LogReplicationEntryMsg transferCompleteAck() {
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE)
                .build();
        return LogReplicationEntryMsg.newBuilder().setMetadata(metadata).build();
    }

    private LogReplicationMetadataResponseMsg metadataResponse(boolean isProcessing) {
        return LogReplicationMetadataResponseMsg.newBuilder().setIsProcessing(isProcessing).build();
    }

    @Test
    public void returnsImmediatelyWithoutPollingWhenAckArrivesInTime() throws Exception {
        CompletableFuture<LogReplicationEntryMsg> ackFuture = CompletableFuture.completedFuture(transferCompleteAck());

        LogReplicationEntryMsg ack = snapshotSender.waitForSnapshotSyncAck(UUID.randomUUID(), ackFuture);

        Assert.assertEquals(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE, ack.getMetadata().getEntryType());
        verify(dataSender, times(0)).sendMetadataRequest();
    }

    @Test
    public void busySinkExtendsWaitInsteadOfFailing() throws Exception {
        // The real ack never arrives on its own within a single DEFAULT_TIMEOUT_MS window; only the
        // busy poll tells the source the sink is still alive and working.
        CompletableFuture<LogReplicationEntryMsg> ackFuture = new CompletableFuture<>();
        when(dataSender.sendMetadataRequest())
                .thenReturn(CompletableFuture.completedFuture(metadataResponse(true)));

        // Resolve the real ack shortly after the first ack-wait window would have elapsed, from a
        // separate thread, proving the wait was extended past one DEFAULT_TIMEOUT_MS instead of the
        // caller giving up on the first timeout.
        ScheduledExecutorService delayedCompletion = Executors.newSingleThreadScheduledExecutor();
        try {
            delayedCompletion.schedule(() -> ackFuture.complete(transferCompleteAck()),
                    DEFAULT_TIMEOUT_MS + 500, TimeUnit.MILLISECONDS);

            LogReplicationEntryMsg ack = snapshotSender.waitForSnapshotSyncAck(UUID.randomUUID(), ackFuture);

            Assert.assertEquals(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE, ack.getMetadata().getEntryType());
            verify(dataSender, times(1)).sendMetadataRequest();
        } finally {
            delayedCompletion.shutdownNow();
        }
    }

    @Test
    public void singleGenuineTimeoutDoesNotFailImmediately() throws Exception {
        // Sink reports no activity at all on the poll, but SNAPSHOT_SYNC_ACK_MAX_RETRIES (6) > 1, so
        // a single silent timeout must not be treated as exhausted yet: the caller keeps waiting and
        // eventually gets the real ack instead of failing after just one timeout.
        CompletableFuture<LogReplicationEntryMsg> ackFuture = new CompletableFuture<>();
        when(dataSender.sendMetadataRequest())
                .thenReturn(CompletableFuture.completedFuture(metadataResponse(false)));

        ScheduledExecutorService delayedCompletion = Executors.newSingleThreadScheduledExecutor();
        try {
            delayedCompletion.schedule(() -> ackFuture.complete(transferCompleteAck()),
                    DEFAULT_TIMEOUT_MS + 500, TimeUnit.MILLISECONDS);

            LogReplicationEntryMsg ack = snapshotSender.waitForSnapshotSyncAck(UUID.randomUUID(), ackFuture);

            Assert.assertEquals(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE, ack.getMetadata().getEntryType());
        } finally {
            delayedCompletion.shutdownNow();
        }
    }
}
