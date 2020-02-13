package org.corfudb.logreplication;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.receive.LogEntryWriter;
import org.corfudb.logreplication.receive.PersistedWriterMetadata;
import org.corfudb.logreplication.receive.ReplicationWriterException;
import org.corfudb.logreplication.receive.StreamsSnapshotWriter;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;

/**
 * A class that represents the Replication Receiver Manager.
 * This is the entry point for destination site.
 */
@Slf4j
public class ReplicationDestinationManager implements DataReceiver {
    private CorfuRuntime runtime;
    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;
    private PersistedWriterMetadata persistedWriterMetadata;
    private RxState rxState;
    private LogReplicationConfig config;

    /**
     * Constructor
     *
     * @param rt Corfu Runtime
     * @param config log replication configuration
     */
    public ReplicationDestinationManager(CorfuRuntime rt, LogReplicationConfig config) {
        this.runtime = rt;
        this.snapshotWriter = new StreamsSnapshotWriter(rt, config);
        this.logEntryWriter = new LogEntryWriter(rt, config);
        this.persistedWriterMetadata = new PersistedWriterMetadata(rt, config.getRemoteSiteID());
        this.rxState = RxState.IDLE_STATE;
        this.logEntryWriter.setTimestamp(persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                persistedWriterMetadata.getLastProcessedLogTimestamp());
    }

    /**
     * Constructor
     *
     * This is temp to solve a dependency issue in application (to be removed as config is required)
     * This requires setLogReplicationConfig to be called.
     *
     * @param rt Corfu Runtime
     */
    public ReplicationDestinationManager(CorfuRuntime rt) {
        this.runtime = rt;
        this.rxState = RxState.IDLE_STATE;
    }

    public void setLogReplicationConfig(LogReplicationConfig config) {
        this.config = config;
        snapshotWriter = new StreamsSnapshotWriter(runtime, config);
        logEntryWriter = new LogEntryWriter(runtime, config);
        persistedWriterMetadata = new PersistedWriterMetadata(runtime, config.getRemoteSiteID());
        logEntryWriter.setTimestamp(persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                persistedWriterMetadata.getLastProcessedLogTimestamp());
    }

    /**
     * Apply message to log.
     *
     * @param message
     */
    public void apply(DataMessage message) {
        // @maxi, how do we distinguish log entry apply from snapshot apply?
        // Buffer data (out of order) and apply
        if (config != null) {
            try {
                if (rxState == RxState.SNAPSHOT_SYNC) {
                    this.snapshotWriter.apply(message);
                } else if (rxState == RxState.LOG_SYN) {
                    this.logEntryWriter.apply(message);
                    persistedWriterMetadata.setLastProcessedLogTimestamp(message.metadata.getTimestamp());
                } else {
                    log.error("it is in the wrong state {}", rxState);
                    throw new ReplicationWriterException("wrong state");
                }
            } catch (ReplicationWriterException e) {
                log.error("Get an exception: ", e);
                throw e;
            }
        } else {
            log.warn("Set LogReplicationConfig for ReplicationDestinationManager");
        }
    }

    /**
     * Apply messages to log.
     *
     * @param messages
     */
    public void apply(List<DataMessage> messages) {
        for (DataMessage msg : messages) {
            apply(msg);
        }
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
     * The end of snapshot fullsync
     */
    public void snapshotDone() {
        //check if the all the expected message has received
        rxState = RxState.LOG_SYN;
        persistedWriterMetadata.setsrcBaseSnapshotDone();
    }

    @Override
    public void receive(DataMessage message) {

    }

    @Override
    public void receive(List<DataMessage> messages) {

    }

    enum RxState {
        IDLE_STATE,
        SNAPSHOT_SYNC,
        LOG_SYN;
    };
}
