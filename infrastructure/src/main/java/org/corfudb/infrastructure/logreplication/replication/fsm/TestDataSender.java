package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.view.Address;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryAckMsg;


/**
 * Test Implementation of Snapshot Data Sender which emulates sending messages by placing directly
 * in an entry queue, and sends ACKs right away.
 *
 * It also emulates the sink's snapshot lease, using the same pure transitions as the real sink,
 * because the source only sends snapshot data to a sink that explicitly admitted the attempt. This
 * fake is lenient where the real sink is strict: it admits a new proposal at once, superseding
 * whatever it was doing, and it needs no checkpoint before reopening admission.
 */
public class TestDataSender implements DataSender {

    private static final long TEST_BUDGET_MS = TimeUnit.HOURS.toMillis(1);

    @Getter
    private final Queue<LogReplicationEntryMsg> entryQueue = new LinkedList<>();

    // Flag which prevents this sender from replying with ACKs so that the test can wait in IN_SNAPSHOT_SYNC state
    // for any operation or validation
    private boolean waitInSnapshotSync;

    // While set, a transferred snapshot stays in its apply phase, so that the test can wait in
    // WAIT_SNAPSHOT_APPLY state.
    @Setter
    private volatile boolean waitInSnapshotApply;

    private SnapshotSyncLeaseRecord lease = SnapshotSyncLease.seedIdle("test-sink", Address.NON_ADDRESS, 0);

    public TestDataSender(boolean waitInSnapshotSync) {
        this.waitInSnapshotSync = waitInSnapshotSync;
    }

    @Override
    public synchronized CompletableFuture<LogReplicationEntryMsg> send(LogReplicationEntryMsg message) {
        LogReplicationEntryType type = message.getMetadata().getEntryType();
        if (type == LogReplicationEntryType.SNAPSHOT_START) {
            return admit(message.getMetadata());
        }
        if (type == LogReplicationEntryType.SNAPSHOT_CANCEL) {
            supersede("cancelled by the source");
            return new CompletableFuture<>();
        }

        if (!message.getData().isEmpty() &&
                (type.equals(LogReplicationEntryType.SNAPSHOT_MESSAGE) ||
                    type.equals(LogReplicationEntryType.LOG_ENTRY_MESSAGE))) {
            // Ignore, do not account Start and End Markers as messages
            entryQueue.add(message);
        }

        CompletableFuture<LogReplicationEntryMsg> cf = new CompletableFuture<>();

        // Do not send an ACK if the test needs to wait in IN_SNAPSHOT_SYNC state.
        if (waitInSnapshotSync) {
            return cf;
        }

        LogReplicationEntryMetadataMsg.Builder ackMetadata =
                LogReplicationEntryMetadataMsg.newBuilder().mergeFrom(message.getMetadata());

        // Emulate behavior from Sink, send ACK per received message
        if (type.equals(LogReplicationEntryType.SNAPSHOT_END)) {
            if (lease.getPhase() == Phase.TRANSFERRING) {
                lease = SnapshotSyncLease.transferred(lease).toBuilder()
                        .setEndSequence(message.getMetadata().getSnapshotSyncSeqNum()).build();
            }
            ackMetadata.setEntryType(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE);
        } else if (type.equals(LogReplicationEntryType.SNAPSHOT_MESSAGE)) {
            ackMetadata.setEntryType(LogReplicationEntryType.SNAPSHOT_REPLICATED);
        } else if (type.equals(LogReplicationEntryType.LOG_ENTRY_MESSAGE)) {
            ackMetadata.setEntryType(LogReplicationEntryType.LOG_ENTRY_REPLICATED);
        } else {
            return cf;
        }

        LogReplicationEntryMsg ack = getLrEntryAckMsg(ackMetadata.build());
        cf.complete(ack);

        return cf;
    }

    /** Admit the proposed attempt right away; a duplicate START is acknowledged again. */
    private CompletableFuture<LogReplicationEntryMsg> admit(LogReplicationEntryMetadataMsg start) {
        if (!(SnapshotSyncLease.matches(lease, start) && SnapshotSyncLease.active(lease))) {
            supersede("superseded by a new proposal");
            try {
                // The real sink insists on the admission epoch it last published. This fake does not
                // make the test wait for another status poll after it superseded an attempt.
                LogReplicationEntryMetadataMsg proposal = start.toBuilder()
                        .setAdmissionEpoch(lease.getAdmissionEpoch()).build();
                lease = SnapshotSyncLease.prepared(SnapshotSyncLease.reserve(lease, proposal,
                        System.currentTimeMillis(), TEST_BUDGET_MS, 0, "test-protection"));
            } catch (SnapshotSyncLease.LeaseRejectedException e) {
                return CompletableFuture.failedFuture(new LogReplicationBusyException(
                        LogReplicationBusyResponseMsg.newBuilder()
                                .setReason(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED)
                                .setSnapshotLease(lease).build()));
            }
        }
        return CompletableFuture.completedFuture(getLrEntryAckMsg(start.toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED)
                .setAttemptGeneration(lease.getGeneration()).build()));
    }

    /** Abandon whatever is in flight and reopen admission at once. */
    private void supersede(String reason) {
        if (SnapshotSyncLease.active(lease)) {
            lease = settle(SnapshotSyncLease.abandon(lease, System.currentTimeMillis(), reason));
        }
    }

    /** Cleanup and recovery of the real sink, collapsed into one step. */
    private static SnapshotSyncLeaseRecord settle(SnapshotSyncLeaseRecord finished) {
        SnapshotSyncLeaseRecord released = SnapshotSyncLease.released(
                SnapshotSyncLease.drained(finished, 0), System.currentTimeMillis());
        return SnapshotSyncLease.recoveredWithoutCheckpoint(released);
    }

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(List<LogReplicationEntryMsg> messages) {

        CompletableFuture<LogReplicationEntryMsg> lastSentMessage = new CompletableFuture<>();

        if (messages != null && !messages.isEmpty()) {
            CompletableFuture<LogReplicationEntryMsg> tmp;

            for (LogReplicationEntryMsg message : messages) {
                tmp = send(message);
                if (message.getMetadata().getEntryType().equals(LogReplicationEntryType.SNAPSHOT_END) ||
                        message.getMetadata().getEntryType().equals(LogReplicationEntryType.LOG_ENTRY_MESSAGE)) {
                    lastSentMessage = tmp;
                }
            }
        }

        return lastSentMessage;
    }

    @Override
    public synchronized CompletableFuture<LogReplicationMetadataResponseMsg> sendMetadataRequest() {
        // Emulate the apply: it completes as soon as the test stops holding it back.
        if (lease.getPhase() == Phase.APPLYING && !waitInSnapshotApply) {
            lease = settle(SnapshotSyncLease.completed(lease));
        }
        boolean transferred = lease.getPhase() == Phase.APPLYING || lease.getOutcome() == Outcome.COMPLETED;
        boolean applied = lease.getOutcome() == Outcome.COMPLETED;
        long snapshot = lease.getSourceSnapshot();
        LogReplicationMetadataResponseMsg response =
                LogReplicationMetadataResponseMsg.newBuilder()
                .setTopologyConfigID(0)
                .setVersion("version")
                .setSnapshotStart(snapshot)
                .setSnapshotTransferred(transferred ? snapshot : Address.NON_ADDRESS)
                .setSnapshotApplied(applied ? snapshot : Address.NON_ADDRESS)
                .setLastLogEntryTimestamp(applied ? snapshot : Address.NON_ADDRESS)
                .setSnapshotLease(lease)
                .build();
        return CompletableFuture.completedFuture(response);
    }

    public synchronized void reset() {
        entryQueue.clear();
        waitInSnapshotSync = false;
    }

    @Override
    public void onError(LogReplicationError error) {}
}
