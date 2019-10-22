package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.Optional;

/**
 * A piece of data needed for the static policy invocation.
 */
@AllArgsConstructor
@Getter
public class StaticPolicyData {
    /**
     * Total addresses that has to be transferred.
     */
    private final List<Long> addresses;
    /**
     * An optional list of the available servers over which one can distribute a load.
     */
    private final Optional<List<String>> availableServers;
    /**
     * A default size of one transfer batch.
     */
    private final int defaultBatchSize;
}
