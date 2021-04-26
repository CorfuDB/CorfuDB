package org.corfudb.infrastructure.compaction;


import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.infrastructure.compaction.exceptions.CompactionException;

import java.util.Optional;

/**
 * A response of a checkpoint task
 */
@Data
@Builder(toBuilder = true)
public class CheckpointTaskResponse {

    public enum Status {
        FAILED,
        FINISHED
    }

    @Builder.Default
    private final Status status = Status.FINISHED;

    private final CheckpointTaskRequest request;

    @Builder.Default
    private final Optional<CompactionException> causeOfFailure = Optional.empty();
}
