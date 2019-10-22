package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import lombok.Builder;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.DynamicPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.IdentityPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.errorpolicy.RemoveServersWithRoundRobin;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.RoundRobinPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.StaticPolicy;

import static lombok.Builder.*;

/**
 * An object that contains the set of policies needed to execute the state transfer.
 */
@Builder
@Getter
public class PolicyStreamProcessorData {
    /**
     * A policy that dictates an initial distribution of batches within a lazy stream, e.g.
     * what addresses will go in every batch,
     * what servers can be used to perform a direct transfer for every batch,
     * what initial latency to use before performing a batch transfer.
     */
    @Default
    private final StaticPolicy initialDistributionPolicy = new RoundRobinPolicy();

    /**
     * A policy that dictates a dynamic (after invocation) distribution of batches
     * within a tail of a lazy stream, e.g.
     * what addresses will go in every batch,
     * what servers can be used to perform a direct transfer for every batch,
     * what dynamic latency to use before performing a transfer of every batch.
     */
    @Default
    private final DynamicPolicy dynamicDistributionPolicy = new IdentityPolicy();

    /**
     * A policy that dictates how the rest of the batches within a tail of a lazy stream
     * will be handled after the batch processor failed to transfer them, e.g.
     * what batches to remove or reschedule to transfer from the set of active servers.
     */
    @Default
    private final DynamicPolicy batchProcessorFailureHandlingPolicy = new RemoveServersWithRoundRobin();
}
