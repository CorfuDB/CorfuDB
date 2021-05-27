package org.corfudb.infrastructure.compaction;

import lombok.Builder;
import lombok.Data;

/**
 * A request to checkpoint a stream with options
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
