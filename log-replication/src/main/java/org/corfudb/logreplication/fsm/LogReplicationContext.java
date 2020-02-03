package org.corfudb.logreplication.fsm;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.Getter;
import org.corfudb.logreplication.transmitter.LogListener;
import org.corfudb.logreplication.transmitter.DataTransmitter;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A context class that contains elements that are shared across the
 * various states of log replication.
 */
@Builder
@Data
public class LogReplicationContext {

    private LogListener snapshotListener;

    private LogListener logEntryListener;

    private LogReplicationConfig config;

    private List<UUID> registeredTablesIDs;

    /*
     Required parameter. Provide compile time enforcement.
     */
    @NonNull
    private CorfuRuntime corfuRuntime;

    private DataTransmitter dataTransmitter;

    // Expect LogReplicationMetadataMap (contains PersistedReplicationMetadata)
    private Map<String, Long> logReplicationMetadataMap;

    /**
     * Executor service for blocking operations.
     */
    private ScheduledExecutorService blockingOpsScheduler;
}