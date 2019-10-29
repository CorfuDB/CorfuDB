package org.corfudb.infrastructure.log.statetransfer.batch;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Optional;

import static org.corfudb.infrastructure.log.statetransfer.batch.BatchResult.FailureStatus.SUCCEEDED;

/**
 * A result of a batch transfer. If completed successfully returns a list of addresses,
 * status SUCCEEDED and an optional destination server.
 * If completed exceptionally, returns a list of empty addresses,
 * status FAILED and an optional destination server.
 */
@Builder
@Getter
@EqualsAndHashCode
public class BatchResult {

    public enum FailureStatus {
        SUCCEEDED,
        FAILED
    }

    @Default
    private final Batch batch = new Batch(ImmutableList.of(), Optional.empty());
    @Default
    private final FailureStatus status = SUCCEEDED;

    /**
     * Gets the batch from this batch result.
     *
     * @return An instance of batch.
     */
    public Batch getBatch() {
        return batch;
    }

}
