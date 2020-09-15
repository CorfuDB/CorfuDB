package org.corfudb.integration;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.infrastructure.logreplication.replication.fsm.ObservableAckMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.integration.DefaultDataControl.DefaultDataControlConfig;
import org.corfudb.runtime.view.Address;

import java.util.List;
import java.util.concurrent.CompletableFuture;

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

    private int droppingNum = 2;

    private int msgCnt = 0;

    // Represents the number of cycles for which we reply that snapshot sync apply has not completed
    private int delayedApplyCycles;
    private int countDelayedApplyCycles = 0;
    private boolean timeoutMetadataResponse = false;

    @Getter
    private ObservableValue errors = new ObservableValue(errorCount);

    private ObservableValue<LogReplicationMetadataResponse> metadataResponseObservable;

    public SourceForwardingDataSender(String destinationEndpoint, LogReplicationConfig config, LogReplicationIT.TestConfig testConfig,
                                      LogReplicationMetadataManager metadataManager,
                                      String pluginConfigFilePath) {
        this.runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(destinationEndpoint)
                .connect();
        this.destinationDataSender = new AckDataSender();
        this.destinationDataControl = new DefaultDataControl(new DefaultDataControlConfig(false, 0));
        this.destinationLogReplicationManager = new LogReplicationSinkManager(runtime.getLayoutServers().get(0), config, metadataManager, pluginConfigFilePath);
        this.ifDropMsg = testConfig.getDropMessageLevel();
        this.delayedApplyCycles = testConfig.getDelayedApplyCycles();
        this.metadataResponseObservable = new ObservableValue<>(null);
        this.timeoutMetadataResponse = testConfig.isTimeoutMetadataResponse();
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message) {
        log.trace("Send message: " + message.getMetadata().getMessageMetadataType() + " for:: " + message.getMetadata().getTimestamp());
        if (ifDropMsg > 0 && msgCnt == droppingNum) {
            log.info("****** Drop msg {} log entry ts {}",  msgCnt, message.getMetadata().timestamp);
            if (ifDropMsg == DROP_MSG_ONCE) {
                droppingNum += DROP_INCREMENT;
            }

            return new CompletableFuture<>();
        }

        final CompletableFuture<LogReplicationEntry> cf = new CompletableFuture<>();

        // Emulate Channel by directly accepting from the destination, whatever is sent by the source manager
        LogReplicationEntry ack = destinationLogReplicationManager.receive(message);
        if (ack != null) {
            cf.complete(ack);
        }
        ackMessages.setValue(ack);
        msgCnt++;
        return cf;
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(List<LogReplicationEntry> messages) {
        CompletableFuture<LogReplicationEntry> lastAckMessage = null;
        CompletableFuture<LogReplicationEntry> tmp;

        for (LogReplicationEntry message :  messages) {
            tmp = send(message);
            if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_END) ||
                    message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                lastAckMessage = tmp;
            }
        }

        try {
            if (lastAckMessage != null) {
                LogReplicationEntry entry = lastAckMessage.get();
                ackMessages.setValue(entry);
            }
        } catch (Exception e) {
            System.out.print("Caught an exception " + e);
        }

        return lastAckMessage;
    }

    @Override
    public CompletableFuture<LogReplicationMetadataResponse> sendMetadataRequest() {
        CompletableFuture<LogReplicationMetadataResponse> completableFuture = new CompletableFuture<>();
        long baseSnapshotTimestamp = destinationDataSender.getSourceManager().getLogReplicationFSM().getBaseSnapshot();
        LogReplicationMetadataResponse response;

        if (delayedApplyCycles > 0 && countDelayedApplyCycles < delayedApplyCycles) {
            countDelayedApplyCycles++;
            log.debug("Received query metadata request, count={}", countDelayedApplyCycles);
            // Reply Snapshot Sync Apply has not completed yet
            response = new LogReplicationMetadataResponse(0, "version", baseSnapshotTimestamp,
                    baseSnapshotTimestamp, Address.NON_ADDRESS, Address.NON_ADDRESS);
        } else {
            if(timeoutMetadataResponse) {
                log.debug("Delay metadata response to cause timeout");
                // For this purpose return an empty completable future which as never completed will time out
                // and reset timeoutMetadataResponse so it returns on next call
                timeoutMetadataResponse = false;
                return new CompletableFuture<>();
            }
            // In test implementation emulate the apply has succeeded and return a LogReplicationMetadataResponse
            response = new LogReplicationMetadataResponse(0, "version", baseSnapshotTimestamp,
                    destinationLogReplicationManager.getLogReplicationMetadataManager().getLastTransferredSnapshotTimestamp(),
                    destinationLogReplicationManager.getLogReplicationMetadataManager().getLastAppliedSnapshotTimestamp(),
                    destinationLogReplicationManager.getLogReplicationMetadataManager().getLastProcessedLogEntryTimestamp());
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

    public ObservableValue<LogReplicationMetadataResponse> getMetadataResponses() {
        return metadataResponseObservable;
    }
}
