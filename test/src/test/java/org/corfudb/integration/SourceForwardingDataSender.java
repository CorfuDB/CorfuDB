package org.corfudb.integration;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.infrastructure.logreplication.replication.fsm.ObservableAckMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.integration.DefaultDataControl.DefaultDataControlConfig;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.view.Address;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This is an implementation of the DataSender (data path layer) used for testing purposes.
 *
 * It emulates the channel by directly forwarding messages to the destination log replication sink manager
 * (for processing).
 */
@Slf4j
public class SourceForwardingDataSender extends AbstractIT implements DataSender {

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
    private int countDelayedApplyCycles = 0;
    private boolean timeoutMetadataResponse = false;

    private LogReplicationIT.TransitionSource callbackFunction;

    @Getter
    private ObservableValue errors = new ObservableValue(errorCount);

    private ObservableValue<LogReplicationMetadataResponseMsg> metadataResponseObservable;

    private long lastAckDropped;

    private CorfuStore sinkCorfuStore;

    private static final String REPLICATION_STATUS_TABLE = LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;

    private LogReplicationSession session = DefaultClusterConfig.getSessions().get(0);

    @SneakyThrows
    public SourceForwardingDataSender(String destinationEndpoint, LogReplicationIT.TestConfig testConfig,
                                      LogReplicationMetadataManager metadataManager,
                                      LogReplicationIT.TransitionSource function,
                                      LogReplicationContext context) {
        this.runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(destinationEndpoint)
                .connect();
        this.destinationDataSender = new AckDataSender();
        this.destinationDataControl = new DefaultDataControl(new DefaultDataControlConfig(
            false, 0));

        // TODO pankti: This test-only constructor can be removed
        this.destinationLogReplicationManager = new LogReplicationSinkManager(runtime.getLayoutServers().get(0),
            metadataManager, session, context);

        this.ifDropMsg = testConfig.getDropMessageLevel();
        this.delayedApplyCycles = testConfig.getDelayedApplyCycles();
        this.metadataResponseObservable = new ObservableValue<>(null);
        this.timeoutMetadataResponse = testConfig.isTimeoutMetadataResponse();
        this.dropACKLevel = testConfig.getDropAckLevel();
        this.callbackFunction = function;
        this.lastAckDropped = Long.MAX_VALUE;
        this.sinkCorfuStore = new CorfuStore(runtime);
        sinkCorfuStore.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationSession.class,
                ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatus.class));
    }

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(LogReplicationEntryMsg message) {
        log.info("Send message: " + message.getMetadata().getEntryType() + " for:: " + message.getMetadata().getTimestamp());
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
            // test negative scenario: when a msg is ignored by Sink, the ACK received should not be for the ignored msg
            ack = destinationLogReplicationManager.receive(changeMsgMetadata(message));
            assertThat(ack.getMetadata().getTimestamp()).isEqualTo(message.getMetadata().getTimestamp());
        } else {
            ack = destinationLogReplicationManager.receive(message);
        }

        //check is_data_consistent flag is set to false on snapshot_start
        if (message.getMetadata().getEntryType().equals(LogReplicationEntryType.SNAPSHOT_START)) {
            checkStatusOnSink(false);
        }

        if (dropAck(ack, message)) {
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

    @Override
    public CompletableFuture<LogReplicationMetadataResponseMsg> sendMetadataRequest() {
        CompletableFuture<LogReplicationMetadataResponseMsg> completableFuture = new CompletableFuture<>();
        long baseSnapshotTimestamp = destinationDataSender.getSourceManager().getLogReplicationFSM().getBaseSnapshot();
        LogReplicationMetadataResponseMsg response;

        if (delayedApplyCycles > 0 && countDelayedApplyCycles < delayedApplyCycles) {
            countDelayedApplyCycles++;
            log.debug("Received query metadata request, count={}", countDelayedApplyCycles);
            // Reply Snapshot Sync Apply has not completed yet
            response = LogReplicationMetadataResponseMsg.newBuilder()
                    .setTopologyConfigID(0)
                    .setVersion("version")
                    .setSnapshotStart(baseSnapshotTimestamp)
                    .setSnapshotTransferred(baseSnapshotTimestamp)
                    .setSnapshotApplied(Address.NON_ADDRESS)
                    .setLastLogEntryTimestamp(Address.NON_ADDRESS)
                    .build();
        } else {
            if(timeoutMetadataResponse) {
                log.debug("Delay metadata response to cause timeout");
                // For this purpose return an empty completable future which as never completed will time out
                // and reset timeoutMetadataResponse so it returns on next call
                timeoutMetadataResponse = false;
                return new CompletableFuture<>();
            }

            LogReplicationMetadata.ReplicationMetadata metadata = destinationLogReplicationManager
                    .getMetadataManager().getReplicationMetadata(session);

            // In test implementation emulate the apply has succeeded and return a LogReplicationMetadataResponse
            response = LogReplicationMetadataResponseMsg.newBuilder()
                    .setTopologyConfigID(0)
                    .setVersion("version")
                    .setSnapshotStart(baseSnapshotTimestamp)
                    .setSnapshotTransferred(metadata.getLastSnapshotTransferred())
                    .setSnapshotApplied(metadata.getLastSnapshotApplied())
                    .setLastLogEntryTimestamp(metadata.getLastLogEntryBatchProcessed())
                    .build();
        }

        metadataResponseObservable.setValue(response);
        completableFuture.complete(response);
        return completableFuture;
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

    /** Change the msg such that Sink ignores the msg. Used to test that the ACK received is not for this msg,
     * i.e., the lastProcessedTs on Sink doesn't change when the msg is ignored.
     **/
    private LogReplicationEntryMsg changeMsgMetadata(LogReplicationEntryMsg message) {
        LogReplicationEntryMsg newMessage = LogReplicationEntryMsg.newBuilder().mergeFrom(message)
                .setMetadata(LogReplication.LogReplicationEntryMetadataMsg.newBuilder().mergeFrom(message.getMetadata())
                        .setTimestamp(message.getMetadata().getTimestamp() + 1)
                        .setPreviousTimestamp(message.getMetadata().getPreviousTimestamp() - 1)
                        .build())
                .build();

        ReplicationMetadata metadata = destinationLogReplicationManager.getMetadataManager()
                .getReplicationMetadata(session);
        assertThat(metadata.getLastLogEntryApplied())
                .isGreaterThanOrEqualTo(newMessage.getMetadata().getPreviousTimestamp());
        assertThat(metadata.getLastLogEntryApplied())
                .isLessThan(newMessage.getMetadata().getTimestamp());

        lastAckDropped = Long.MAX_VALUE;

        return newMessage;
    }

    public void checkStatusOnSink(boolean expectedDataConsistent) {
        try (TxnContext txn = sinkCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            ReplicationStatus status = (ReplicationStatus) txn.getRecord(REPLICATION_STATUS_TABLE, session).getPayload();
            assertThat(status.getSinkStatus().getDataConsistent()).isEqualTo(expectedDataConsistent);
        }
    }

    public void resetTestConfig(LogReplicationIT.TestConfig testConfig) {
        this.ifDropMsg = testConfig.getDropMessageLevel();
        this.delayedApplyCycles = testConfig.getDelayedApplyCycles();
        this.timeoutMetadataResponse = testConfig.isTimeoutMetadataResponse();
        this.dropACKLevel = testConfig.getDropAckLevel();
    }
}
