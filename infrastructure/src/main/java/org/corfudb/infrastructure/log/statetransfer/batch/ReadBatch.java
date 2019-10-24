package org.corfudb.infrastructure.log.statetransfer.batch;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;
import java.util.Optional;

import static lombok.Builder.Default;

/**
 * A batch of data read from the cluster (or from a specific node) and
 * an optional destination server where the corresponding data is located.
 */
@Getter
@ToString
@Builder
public class ReadBatch {
    public enum FailureStatus {
        SUCCEEDED,
        FAILED
    }

    /**
     * A complete batch of a data read from the cluster (or from a specific node).
     */
    @Default
    private final List<LogData> data = ImmutableList.of();

    /**
     * Addresses for which the data was not read.
     */
    @Default
    private final List<Long> failedAddress = ImmutableList.of();
    /**
     * This is field is optional because the batch can be read from the cluster
     * consistency protocol rather than from the specific destination.
     */
    @Default
    private final Optional<String> destination = Optional.empty();

    /**
     * A status of success or failure after read was performed.
     */
    @Default
    private final FailureStatus status = FailureStatus.SUCCEEDED;

    /**
     * Transform this read batch into a new batch.
     *
     * @return An instance of batch.
     */
    public Batch toBatch() {
        return new Batch(failedAddress, destination);
    }


}
