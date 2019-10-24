package org.corfudb.infrastructure.log.statetransfer.batch;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private final List<Long> addresses = ImmutableList.of();
    @Default
    private final Optional<String> destinationServer = Optional.empty();
    @Default
    private final FailureStatus status = SUCCEEDED;

    /**
     * Transform this batch result into a new batch.
     *
     * @return An instance of batch.
     */
    public Batch toBatch() {
        return new Batch(addresses, destinationServer);
    }

}
