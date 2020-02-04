package org.corfudb.logreplication.fsm;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import org.corfudb.logreplication.transmitter.LogEntryListener;
import org.corfudb.logreplication.transmitter.DataTransmitter;
import org.corfudb.logreplication.transmitter.SnapshotListener;
import org.corfudb.logreplication.transmitter.SnapshotReader;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * A context class that contains elements that are shared across the
 * various states of log replication.
 */
@Builder
@Data
public class LogReplicationContext {

    private LogEntryListener logEntryListener;

    private SnapshotReader snapshotReader;

    private LogReplicationConfig config;

    private List<UUID> registeredTablesIDs;

    /*
     Required parameter. Provide compile time enforcement.
     */
    @NonNull
    private CorfuRuntime corfuRuntime;

    private LogReplicationFSM logReplicationFSM;

    // Expect LogReplicationMetadataMap (contains PersistedReplicationMetadata)
    private Map<String, Long> logReplicationMetadataMap;

    /**
     * Executor service for blocking operations.
     */
    private ExecutorService stateMachineWorker;
}