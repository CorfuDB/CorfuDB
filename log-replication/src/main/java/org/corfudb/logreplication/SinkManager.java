package org.corfudb.logreplication;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.message.MessageMetadata;
import org.corfudb.logreplication.message.MessageType;
import org.corfudb.logreplication.receive.LogEntryWriter;
import org.corfudb.logreplication.receive.PersistedWriterMetadata;
import org.corfudb.logreplication.receive.ReplicationWriterException;
import org.corfudb.logreplication.receive.StreamsSnapshotWriter;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;
import java.util.UUID;

/**
 * This class represents the Log Replication Manager at the destination.
 *
 * It is the entry point for log replication at the receiver.
 *
 * */
@Slf4j
public class SinkManager implements DataReceiver {
    private CorfuRuntime runtime;
    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;
    private PersistedWriterMetadata persistedWriterMetadata;
    private RxState rxState;
    private DataSender dataSender;
    private DataControl dataControl;
    private LogReplicationConfig config;

    /**
     * Constructor
     *
     * @param rt Corfu Runtime
     * @param config log replication configuration
     */
    public SinkManager(CorfuRuntime rt, DataSender dataSender, DataControl dataControl, LogReplicationConfig config) {
        this(rt, dataSender, dataControl);
        setLogReplicationConfig(config);
    }

    /**
     * Constructor
     *
     * This is temp to solve a dependency issue in application (to be removed as config is required)
     * This requires setLogReplicationConfig to be called.
     *
     * @param rt Corfu Runtime
     */
    public SinkManager(CorfuRuntime rt, DataSender dataSender, DataControl dataControl) {
        this.runtime = rt;
        this.rxState = RxState.IDLE_STATE;
        this.dataSender = dataSender;
        this.dataControl = dataControl;
    }

    public void setLogReplicationConfig(LogReplicationConfig config) {
        this.config = config;
        snapshotWriter = new StreamsSnapshotWriter(runtime, config);
        logEntryWriter = new LogEntryWriter(runtime, dataSender, config);
        persistedWriterMetadata = new PersistedWriterMetadata(runtime, config.getRemoteSiteID());
        logEntryWriter.setTimestamp(persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                persistedWriterMetadata.getLastProcessedLogTimestamp());
    }

    /**
     * Signal the manager a snapshot sync is about to start. This is required to reset previous states.
     */
    public void snapshotStart(long srcSnapTimestamp) {
        rxState = RxState.SNAPSHOT_SYNC;
        persistedWriterMetadata.setsrcBaseSnapshotStart(srcSnapTimestamp);

        // Signal start of snapshot sync to the receive, so data can be cleared.
        this.snapshotWriter.reset(srcSnapTimestamp);
    }

    /**
     * The end of snapshot sync
     */
    public void snapshotDone(UUID snapshotSyncId) {
        //check if the all the expected message has received
        rxState = RxState.LOG_SYNC;
        persistedWriterMetadata.setsrcBaseSnapshotDone();

        // Prepare Snapshot Sync
        MessageMetadata metadata = new MessageMetadata(MessageType.SNAPSHOT_REPLICATED,
                persistedWriterMetadata.getLastProcessedLogTimestamp(),
                persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                snapshotSyncId);

        dataSender.send(new DataMessage(metadata));
    }

    @Override
    public void receive(DataMessage message) {
        // @maxi, how do we distinguish log entry apply from snapshot apply?
        // Buffer data (out of order) and apply
        if (config != null) {
            try {
                if (rxState == RxState.SNAPSHOT_SYNC &&
                        message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_MESSAGE) {
                    this.snapshotWriter.apply(message);
                } else if (rxState == RxState.LOG_SYNC &&
                    message.getMetadata().getMessageMetadataType() == MessageType.LOG_ENTRY_MESSAGE) {
                    this.logEntryWriter.apply(message);
                    persistedWriterMetadata.setLastProcessedLogTimestamp(message.metadata.getTimestamp());
                } else {
                    log.error("it is in the wrong state {}", rxState);
                    throw new ReplicationWriterException("wrong state");
                }
            } catch (ReplicationWriterException e) {
                log.error("Get an exception: ", e);
                dataControl.requestSnapshotSync();
                log.info("Requested Snapshot Sync.");
            }
        } else {
            log.warn("Set LogReplicationConfig for Sync");
        }
    }

    @Override
    public void receive(List<DataMessage> messages) {
        for (DataMessage msg : messages) {
            receive(msg);
        }
    }

    enum RxState {
        IDLE_STATE,
        SNAPSHOT_SYNC,
        LOG_SYNC
    };
}
