package org.corfudb.infrastructure.log.statetransfer.batch;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.Optional;

/**
 * A batch of addresses, small enough to get transferred within one rpc call and
 * an optional destination server where the corresponding data is located.
 */
@AllArgsConstructor
@Getter
public class Batch {
    private final List<Long> addresses;
    private final Optional<String> destination;
}
