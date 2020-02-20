package org.corfudb.integration;

import lombok.Getter;
import org.corfudb.logreplication.DataSender;
import org.corfudb.logreplication.SinkManager;
import org.corfudb.logreplication.SourceManager;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.fsm.ObservableValue;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.message.LogReplicationEntry;
import org.corfudb.logreplication.send.LogReplicationError;
import org.corfudb.runtime.CorfuRuntime;

import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import java.util.Random;
import java.util.UUID;
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

    private boolean ifDropMsg = false;

    Random random = new Random();

    private int firstDrop = 0;

    final static int DROP_INCREMENT = 4;

    @Getter
    private ObservableValue errors = new ObservableValue(errorCount);

    public SourceForwardingDataSender(String destinationEndpoint, LogReplicationConfig config, boolean ifDropMsg) {
        this.runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(destinationEndpoint)
                .connect();
        this.destinationDataSender = new AckDataSender();
        this.destinationDataControl = new DefaultDataControl(false);
        this.destinationLogReplicationManager = new SinkManager(runtime, destinationDataSender, destinationDataControl);
        this.destinationLogReplicationManager.setLogReplicationConfig(config);
        this.channelExecutorWorkers = Executors.newSingleThreadExecutor();
        this.ifDropMsg = ifDropMsg;
    }

    /*
     * ---------------------- SNAPSHOT SYNC METHODS --------------------------
     */
    @Override
    public boolean send(DataMessage message, UUID snapshotSyncId, boolean completed) {
        // Emulate Channel by directly accepting from the destination, whatever is sent by the source manager
        receivedMessages++;
        if (receivedMessages == 1) {
            channelExecutorWorkers.execute(() -> destinationLogReplicationManager.startSnapshotApply());
        }

        channelExecutorWorkers.execute(() -> destinationLogReplicationManager.receive(message));

        if (completed) {
            channelExecutorWorkers.execute(() -> destinationLogReplicationManager.completeSnapshotApply());
        }
        return completed;
    }

    @Override
    public boolean send(List<DataMessage> messages, UUID snapshotSyncId, boolean completed) {
        messages.forEach(msg -> send(msg, snapshotSyncId, completed));
        return true;
    }

    @Override
    public void onError(LogReplicationError error, UUID snapshotSyncId) { fail("Error received for snapshot sync");
    }

    /*
     * ---------------------- LOG ENTRY SYNC METHODS --------------------------
     */
    @Override
    public boolean send(DataMessage message) {
        LogReplicationEntry logReplicationEntry = LogReplicationEntry.deserialize(message.getData());

        if (logReplicationEntry.metadata.timestamp == firstDrop) {
            System.out.println("******drop log entry " + logReplicationEntry.metadata.timestamp);
            firstDrop += DROP_INCREMENT;
            return true;
        }

        // Emulate Channel by directly accepting from the destination, whatever is sent by the source manager
        channelExecutorWorkers.execute(() -> destinationLogReplicationManager.receive(message));
        return true;
    }

    @Override
    public boolean send(List<DataMessage> messages) {
        messages.forEach(msg -> send(msg));
        return true;
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
}
