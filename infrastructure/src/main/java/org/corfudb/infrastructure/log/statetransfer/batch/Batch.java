package org.corfudb.infrastructure.log.statetransfer.batch;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Optional;

/**
 * A batch of addresses, small enough to get transferred within one rpc call and
 * an optional destination server where the corresponding data is located.
 */
@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
@Builder
public class Batch {
    /**
     * A batch of addresses, small enough to get transferred within one rpc call.
     */
    @Builder.Default
    private final List<Long> addresses = ImmutableList.of();
    /**
     * This is field is optional because the batch can be read from the cluster
     * consistency protocol rather than from the specific destination.
     */
    @Builder.Default
    private final Optional<String> destination = Optional.empty();
}
