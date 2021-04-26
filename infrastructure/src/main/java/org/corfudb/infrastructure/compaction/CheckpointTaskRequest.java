package org.corfudb.infrastructure.compaction;

import lombok.Builder;
import lombok.Data;

import java.util.Optional;

/**
 * A request to checkpoint a stream from a particular destination node
 * (if {@link #destinationNode} is present) or via a replication protocol otherwise.
 */
@Data
@Builder(toBuilder = true)
public class CheckpointTaskRequest {

    /**
     * The namespace of the table.
     */
    private final String namespace;

    /**
     * The name of the table.
     */
    private final String tableName;

    /**
     * The id of the compaction task.
     */
    private final String compactionTaskId;

    /**
     * A snapshot to perform the checkpoint task.
     */
    private final SafeSnapshot safeSnapshot;

    /**
     * Options for checkpointing.
     */
    private final CheckpointOptions options;
}
