package org.corfudb.integration;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.SinkManager;
import org.corfudb.logreplication.SourceManager;
import org.corfudb.infrastructure.logreplication.LogReplicationError;
import org.corfudb.logreplication.fsm.ObservableAckMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.integration.DefaultDataControl.DefaultDataControlConfig;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SourceForwardingDataSender implements DataSender {
    // Runtime to remote/destination Corfu Server
    private CorfuRuntime runtime;

    // Manager in remote/destination site, to emulate the channel, we instantiate the destination receiver
    private SinkManager destinationLogReplicationManager;

    // Destination DataSender
    private AckDataSender destinationDataSender;

    // Destination DataControl
    private DefaultDataControl destinationDataControl;

    private ExecutorService channelExecutorWorkers;

    private int receivedMessages = 0;

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

    private int ifDropMsg = 0;

    final static int DROP_INCREMENT = 4;

    private int firstDrop = DROP_INCREMENT;

    @Getter
    private ObservableValue errors = new ObservableValue(errorCount);

    public SourceForwardingDataSender(String destinationEndpoint, LogReplicationConfig config, int ifDropMsg) {
        this.runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(destinationEndpoint)
                .connect();
        this.destinationDataSender = new AckDataSender();
        this.destinationDataControl = new DefaultDataControl(new DefaultDataControlConfig(false, 0));
        this.destinationLogReplicationManager = new SinkManager(runtime.getLayoutServers().get(0), config);
        this.channelExecutorWorkers = Executors.newSingleThreadExecutor();
        this.ifDropMsg = ifDropMsg;
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message) {
        //System.out.println("Send message: " + message.getMetadata().getMessageMetadataType() + " for:: " + message.getMetadata().getTimestamp());
        if (ifDropMsg > 0 && message.getMetadata().timestamp == firstDrop) {
            System.out.println("****** Drop log entry " + message.getMetadata().timestamp);
            if (ifDropMsg == DROP_MSG_ONCE) {
                firstDrop += DROP_INCREMENT;
            }

            return new CompletableFuture<>();
        }

        final CompletableFuture<LogReplicationEntry> cf = new CompletableFuture<>();

        // Emulate Channel by directly accepting from the destination, whatever is sent by the source manager
        // channelExecutorWorkers.execute(() -> destinationLogReplicationManager.receive(message));
        LogReplicationEntry ack = destinationLogReplicationManager.receive(message);
        cf.complete(ack);
        ackMessages.setValue(ack);
        return cf;
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(List<LogReplicationEntry> messages) {
        CompletableFuture<LogReplicationEntry> lastSentMessage = null;
        CompletableFuture<LogReplicationEntry> tmp;

        for (LogReplicationEntry message :  messages) {
            tmp = send(message);
            if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_END) ||
                    message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                lastSentMessage = tmp;
            }
        }

        try {
            if (lastSentMessage != null) {
                LogReplicationEntry entry = lastSentMessage.get();
                ackMessages.setValue(entry);
            }
        } catch (Exception e) {
            System.out.print("Caught an exception " + e);
        }

        return lastSentMessage;
    }

    @Override
    public void onError(LogReplicationError error) {
        errorCount++;
        errors.setValue(errorCount);
    }

    /*
     * Auxiliary Methods
     */
    public void setSourceManager(SourceManager sourceManager) {
        destinationDataSender.setSourceManager(sourceManager);
        destinationDataControl.setSourceManager(sourceManager);
    }

    // Used for testing purposes to access the SinkManager in Test
    public SinkManager getSinkManager() {
        return destinationLogReplicationManager;
    }

    public CorfuRuntime getWriterRuntime() {
        return this.runtime;
    }

    public void shutdown() {
        if (destinationDataSender != null && destinationDataSender.getSourceManager() != null) {
            destinationDataSender.getSourceManager().shutdown();
        }

        if (destinationLogReplicationManager != null) {
            destinationLogReplicationManager.shutdown();
        }
    }
}
