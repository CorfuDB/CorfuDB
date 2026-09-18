package org.corfudb.integration;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.fsm.ObservableAckMsg;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.integration.DefaultDataControl.DefaultDataControlConfig;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.view.Address;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This is an implementation of the DataSender (data path layer) used for testing purposes.
 *
 * It emulates the channel by directly forwarding messages to the destination log replication sink manager
 * (for processing).
 */
@Slf4j
public class SourceForwardingDataSender implements DataSender {

    private final static int DROP_INCREMENT = 4;

    // Runtime to remote/destination Corfu Server
    private CorfuRuntime runtime;

    // Manager in remote/destination site, to emulate the channel, we instantiate the destination receiver
    private LogReplicationSinkManager destinationLogReplicationManager;

    // Destination DataSender
    private AckDataSender destinationDataSender;

    // Destination DataControl
    private DefaultDataControl destinationDataControl;

    private int errorCount = 0;

    @VisibleForTesting
    @Getter
    private ObservableAckMsg ackMessages = new ObservableAckMsg();

    /*
     * 0: no message drop
     * 1: drop some message once
     * 2: drop a particular message 5 times to trigger a timeout error
     */
    final public static int DROP_MSG_ONCE = 1;

    private int ifDropMsg;

    private int dropACKLevel;

    private int droppingNum = 2;

    private int droppingAcksNum = 2;

    private int msgCnt = 0;

    // Represents the number of cycles for which we reply that snapshot sync apply has not completed
    private int delayedApplyCycles;
    @Getter
    private int countDelayedApplyCycles = 0;
    private boolean timeoutMetadataResponse = false;
    private volatile CompletableFuture<LogReplicationEntryMsg> snapshotTransferAck;
    // Typed BUSY replies of the sink that were passed on to the source
    @Getter
    private final AtomicInteger busyReplies = new AtomicInteger();
    @Getter
    private final AtomicInteger applyMetadataTimeouts = new AtomicInteger();
    @Getter
    private final AtomicInteger initialMetadataTimeouts = new AtomicInteger();

    private LogReplicationIT.TransitionSource callbackFunction;

    @Getter
    private ObservableValue errors = new ObservableValue(errorCount);

    private ObservableValue<LogReplicationMetadataResponseMsg> metadataResponseObservable;

    private long lastAckDropped;

    private CorfuStore standbyCorfuStore;

    private final String destinationClusterId;

    private static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";

    private LogReplicationIT.TestConfig testConfig;

    @Getter
    private volatile int numStartMsgsDropped;

    private int delayTransferCompleteAckMs;

    private boolean dropAllAfterSnapshotStart;

    private static final int SINK_READY_TIMEOUT_MS = 60_000;

    @VisibleForTesting
    @Getter
    private final AtomicInteger metadataRequestCount = new AtomicInteger(0);

    private final ScheduledExecutorService delayedAckExecutor = Executors.newSingleThreadScheduledExecutor();

    @SneakyThrows
    public SourceForwardingDataSender(String destinationEndpoint, LogReplicationConfig config, LogReplicationIT.TestConfig testConfig,
                                      LogReplicationMetadataManager metadataManager,
                                      String pluginConfigFilePath, LogReplicationIT.TransitionSource function) {
        this.runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(destinationEndpoint)
                .connect();
        this.destinationDataSender = new AckDataSender();
        this.destinationDataControl = new DefaultDataControl(new DefaultDataControlConfig(false, 0));
        this.destinationLogReplicationManager = new LogReplicationSinkManager(runtime.getLayoutServers().get(0), config, metadataManager, pluginConfigFilePath);
        if (testConfig.getLeaseTiming() != null) {
            this.destinationLogReplicationManager.configureSnapshotLifecycle(testConfig.getLeaseTiming());
        }
        // In a deployment the discovery service grants leadership. A sink admits nothing before it
        // leads, and incremental traffic only once it installed its writer for the state it found.
        this.destinationLogReplicationManager.setLeadership(true);
        awaitSinkReady();
        this.ifDropMsg = testConfig.getDropMessageLevel();
        this.delayedApplyCycles = testConfig.getDelayedApplyCycles();
        this.metadataResponseObservable = new ObservableValue<>(null);
        this.timeoutMetadataResponse = testConfig.isTimeoutMetadataResponse();
        this.dropACKLevel = testConfig.getDropAckLevel();
        this.delayTransferCompleteAckMs = testConfig.getDelayTransferCompleteAckMs();
        this.dropAllAfterSnapshotStart = testConfig.isDropAllAfterSnapshotStart();
        this.callbackFunction = function;
        this.lastAckDropped = Long.MAX_VALUE;
        this.standbyCorfuStore = new CorfuStore(runtime);
        standbyCorfuStore.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
        this.destinationClusterId = testConfig.getRemoteClusterId();
        this.testConfig = testConfig;
    }

    private void awaitSinkReady() throws InterruptedException {
        long deadline = System.currentTimeMillis() + SINK_READY_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            SnapshotSyncLeaseRecord lease = destinationLogReplicationManager.getSnapshotLease();
            boolean acquired = lease.getPhase() != SnapshotSyncLeaseRecord.Phase.NOT_READY;
            boolean incrementalPending = lease.getOutcome() == SnapshotSyncLeaseRecord.Outcome.COMPLETED
                    && !destinationLogReplicationManager.isIncrementalSyncAdmitted();
            if (acquired && !incrementalPending) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new IllegalStateException("The sink did not acquire its snapshot lease: "
                + destinationLogReplicationManager.getSnapshotLease());
    }

    /**
     * Emulates the transport. What the sink cannot process right now it answers with a typed BUSY
     * reply, which the transport hands to the source as a failed request; a dropped message or
     * acknowledgement is a request that never completes.
     */
    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(LogReplicationEntryMsg message) {
        try {
            return forward(message);
        } catch (LogReplicationBusyException busy) {
            busyReplies.incrementAndGet();
            log.debug("Sink replied BUSY {} to {}", busy.getResponse().getReason(), message.getMetadata().getEntryType());
            return CompletableFuture.failedFuture(busy);
        }
    }

    private CompletableFuture<LogReplicationEntryMsg> forward(LogReplicationEntryMsg message) {
        if (message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_START) {
            snapshotTransferAck = null;
        }
        // Simulate a source that has gone silent (network partition, crash) right after starting a
        // snapshot sync: nothing past SNAPSHOT_START ever reaches the sink.
        if (dropAllAfterSnapshotStart && message.getMetadata().getEntryType() != LogReplicationEntryType.SNAPSHOT_START) {
            return new CompletableFuture<>();
        }

        // Check if the SNAPSHOT_START message must be dropped
        if (testConfig.isDropSnapshotStartMsg() && message.getMetadata().getEntryType() ==
                LogReplicationEntryType.SNAPSHOT_START) {
            // If a limited number of START messages must be dropped, drop them only if the number is yet to be
            // reached
            if (numStartMsgsDropped < testConfig.getNumDropsForSnapshotStart()) {
                numStartMsgsDropped++;
                return new CompletableFuture<>();
            }
        }

        if (message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_START
                || message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_CANCEL) {
            // Admission and cancellation are control traffic: they are neither subject to the
            // data-plane fault injection below nor counted as data acknowledgements.
            LogReplicationEntryMsg reply = destinationLogReplicationManager.receive(message);
            return reply == null ? new CompletableFuture<>() : CompletableFuture.completedFuture(reply);
        }

        log.trace("Send message: " + message.getMetadata().getEntryType() + " for:: " + message.getMetadata().getTimestamp());
        if (ifDropMsg > 0 && msgCnt == droppingNum || dropACKLevel == 2 && message.getMetadata().getTimestamp() >= lastAckDropped) {
            log.info("****** Drop msg {} log entry ts {}",  msgCnt, message.getMetadata().getTimestamp());
            if (ifDropMsg == DROP_MSG_ONCE) {
                droppingNum += DROP_INCREMENT;
            }

            return new CompletableFuture<>();
        }

        final CompletableFuture<LogReplicationEntryMsg> cf = new CompletableFuture<>();
        LogReplicationEntryMsg ack;

        // Emulate Channel by directly accepting from the destination, whatever is sent by the source manager
        if (lastAckDropped < message.getMetadata().getTimestamp()) {
            // resend msg multiple times and assert ack is received for every resend
            for (int resentTme = 0; resentTme < 2; resentTme++) {
                ack = destinationLogReplicationManager.receive(message);
                assertThat(ack.getMetadata().getTimestamp()).isEqualTo(message.getMetadata().getTimestamp());
            }
            // test negative scenario: send a msg with lower 'previousTimestamp' and no new data to apply.  Verify
            // that last timestamp in the ACK remains the same.
            ack = destinationLogReplicationManager.receive(changeMsgMetadata(message));
            assertThat(ack.getMetadata().getTimestamp()).isEqualTo(message.getMetadata().getTimestamp());
        } else {
            ack = destinationLogReplicationManager.receive(message);
        }

        if (ack != null && ack.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE) {
            snapshotTransferAck = cf;
        }
        if (dropAck(ack, message)) {
            return cf;
        }

        if (delayTransferCompleteAckMs > 0 && ack != null
                && ack.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE) {
            // Sink has already processed the message; only delay delivery of the ack back to the
            // source, to simulate a slow-but-alive sink without actually slowing down processing.
            log.info("Delaying delivery of SNAPSHOT_TRANSFER_COMPLETE ack by {} ms", delayTransferCompleteAckMs);
            LogReplicationEntryMsg delayedAck = ack;
            delayedAckExecutor.schedule(() -> {
                ackMessages.setValue(delayedAck);
                cf.complete(delayedAck);
            }, delayTransferCompleteAckMs, TimeUnit.MILLISECONDS);
            msgCnt++;
            return cf;
        }

        if (ack != null) {
            cf.complete(ack);
        }
        ackMessages.setValue(ack);
        msgCnt++;
        return cf;
    }

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(List<LogReplicationEntryMsg> messages) {
        CompletableFuture<LogReplicationEntryMsg> lastAckMessage = null;
        CompletableFuture<LogReplicationEntryMsg> tmp;

        for (LogReplicationEntryMsg message :  messages) {
            tmp = send(message);
            if (message.getMetadata().getEntryType().equals(LogReplicationEntryType.SNAPSHOT_END) ||
                    message.getMetadata().getEntryType().equals(LogReplicationEntryType.LOG_ENTRY_MESSAGE)) {
                lastAckMessage = tmp;
            }
        }

        try {
            if (lastAckMessage != null) {
                LogReplicationEntryMsg entry = lastAckMessage.get();
                ackMessages.setValue(entry);
            }
        } catch (Exception e) {
            System.out.print("Caught an exception " + e);
        }

        return lastAckMessage;
    }

    /**
     * The sink publishes a committed view of its status, lease included, once per second and
     * independently of source polls; a poll only reads that view. The faults injected here are the
     * ones a transport can cause (a reply that never arrives) and an apply that takes several polls.
     */
    @Override
    public CompletableFuture<LogReplicationMetadataResponseMsg> sendMetadataRequest() {
        metadataRequestCount.incrementAndGet();
        if (testConfig.isTimeoutInitialMetadataResponse() && initialMetadataTimeouts.compareAndSet(0, 1)) {
            return new CompletableFuture<>();
        }
        LogReplicationMetadataResponseMsg response = destinationLogReplicationManager
                .getLogReplicationMetadataManager().getCachedSnapshotStatus();
        long baseSnapshotTimestamp = destinationDataSender.getSourceManager().getLogReplicationFSM().getBaseSnapshot();
        boolean transferAcknowledged = isSnapshotTransferAcknowledged(baseSnapshotTimestamp);

        if (transferAcknowledged && delayedApplyCycles > 0 && countDelayedApplyCycles < delayedApplyCycles) {
            countDelayedApplyCycles++;
            log.debug("Received query metadata request, count={}", countDelayedApplyCycles);
            // Reply that the apply of this attempt has not completed yet
            response = response.toBuilder()
                    .setSnapshotApplied(Address.NON_ADDRESS)
                    .setLastLogEntryTimestamp(Address.NON_ADDRESS)
                    .setSnapshotLease(response.getSnapshotLease().toBuilder()
                            .setPhase(SnapshotSyncLeaseRecord.Phase.APPLYING)
                            .setOutcome(SnapshotSyncLeaseRecord.Outcome.NONE)
                            .setProtectionHeld(true))
                    .build();
        } else if (transferAcknowledged && timeoutMetadataResponse) {
            log.debug("Delay metadata response to cause timeout");
            // For this purpose return an empty completable future which as never completed will time out
            // and reset timeoutMetadataResponse so it returns on next call
            timeoutMetadataResponse = false;
            applyMetadataTimeouts.incrementAndGet();
            return new CompletableFuture<>();
        }

        metadataResponseObservable.setValue(response);
        return CompletableFuture.completedFuture(response);
    }

    public boolean isSnapshotTransferAcknowledged(long snapshot) {
        CompletableFuture<LogReplicationEntryMsg> pending = snapshotTransferAck;
        if (pending == null || pending.isCompletedExceptionally()) {
            return false;
        }
        LogReplicationEntryMsg ack = pending.getNow(null);
        return ack != null && ack.getMetadata().getSnapshotTimestamp() == snapshot;
    }

    @Override
    public void onError(LogReplicationError error) {
        errorCount++;
        errors.setValue(errorCount);
        log.trace("OnError :: code={}, description={}", error.getCode(), error.getDescription());
    }

    /*
     * Auxiliary Methods
     */
    public void setSourceManager(LogReplicationSourceManager sourceManager) {
        destinationDataSender.setSourceManager(sourceManager);
        destinationDataControl.setSourceManager(sourceManager);
    }

    // Used for testing purposes to access the LogReplicationSinkManager in Test
    public LogReplicationSinkManager getSinkManager() {
        return destinationLogReplicationManager;
    }

    public void shutdown() {
        if (destinationDataSender != null && destinationDataSender.getSourceManager() != null) {
            destinationDataSender.getSourceManager().shutdown();
        }

        if (destinationLogReplicationManager != null) {
            destinationLogReplicationManager.shutdown();
        }

        if (runtime != null) {
            runtime.shutdown();
        }

        delayedAckExecutor.shutdownNow();
    }

    public ObservableValue<LogReplicationMetadataResponseMsg> getMetadataResponses() {
        return metadataResponseObservable;
    }

    private boolean dropAck(LogReplicationEntryMsg ack, LogReplicationEntryMsg message){
        if (dropACKLevel > 0 && msgCnt == droppingAcksNum) {
            log.info("****** Drop ACK {} for log entry ts {}", ack, message.getMetadata().getTimestamp());
            if (dropACKLevel == DROP_MSG_ONCE) {
                droppingAcksNum += DROP_INCREMENT;
            }

            if (dropACKLevel == 2) {
                lastAckDropped = message.getMetadata().getTimestamp();
                callbackFunction.changeState();
            }
            return true;
        }
        return false;
    }

    /** Change the msg such that Sink ignores the msg (previousTimestamp and current timestamp are decremented by 1,
     * which means no new messages to apply). Used to test that the ACK received is not for this msg,
     * i.e., the lastProcessedTs on Sink doesn't change when the msg is ignored.
     **/
    private LogReplicationEntryMsg changeMsgMetadata(LogReplicationEntryMsg message) {
        LogReplicationEntryMsg newMessage = LogReplicationEntryMsg.newBuilder().mergeFrom(message)
                .setMetadata(LogReplication.LogReplicationEntryMetadataMsg.newBuilder().mergeFrom(message.getMetadata())
                        .setTimestamp(message.getMetadata().getTimestamp() - 1)
                        .setPreviousTimestamp(message.getMetadata().getPreviousTimestamp() - 1)
                        .build())
                .build();

        assertThat(destinationLogReplicationManager.getLogReplicationMetadataManager()
                .getLastProcessedLogEntryBatchTimestamp())
                .isGreaterThan(newMessage.getMetadata().getPreviousTimestamp());
        assertThat(destinationLogReplicationManager.getLogReplicationMetadataManager()
                .getLastProcessedLogEntryBatchTimestamp())
                .isGreaterThan(newMessage.getMetadata().getTimestamp());
        assertThat(destinationLogReplicationManager.getLogReplicationMetadataManager()
                .getLastProcessedLogEntryBatchTimestamp())
                .isEqualTo(message.getMetadata().getTimestamp());

        lastAckDropped = Long.MAX_VALUE;

        return newMessage;
    }

    public void checkStatusOnStandby(boolean expectedDataConsistent) {
        if (destinationClusterId == null) {
            return;
        }
        LogReplicationMetadata.ReplicationStatusKey standbyClusterId = LogReplicationMetadata.ReplicationStatusKey.newBuilder()
                .setClusterId(destinationClusterId)
                .build();
        try (TxnContext txn = standbyCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            LogReplicationMetadata.ReplicationStatusVal standbyStatus = (LogReplicationMetadata.ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, standbyClusterId).getPayload();
            assertThat(standbyStatus.getDataConsistent()).isEqualTo(expectedDataConsistent);
        }
    }

    public void resetTestConfig(LogReplicationIT.TestConfig testConfig) {
        this.ifDropMsg = testConfig.getDropMessageLevel();
        this.delayedApplyCycles = testConfig.getDelayedApplyCycles();
        this.timeoutMetadataResponse = testConfig.isTimeoutMetadataResponse();
        this.dropACKLevel = testConfig.getDropAckLevel();
        this.delayTransferCompleteAckMs = testConfig.getDelayTransferCompleteAckMs();
        this.dropAllAfterSnapshotStart = testConfig.isDropAllAfterSnapshotStart();
    }
}
