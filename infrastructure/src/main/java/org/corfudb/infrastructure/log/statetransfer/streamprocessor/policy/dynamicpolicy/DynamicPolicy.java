package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy;

import org.corfudb.infrastructure.log.statetransfer.batch.Batch;

import java.util.Optional;
import java.util.function.Supplier;

@FunctionalInterface
public interface DynamicPolicy {

    /**
     * Based on the aggregated statistics so far,
     * apply the function to the tail of the batch stream
     * to transform it.
     * @param data A dynamic policy data.
     * @return A supplier that transforms the tail.
     * Should be utilized in the Stream.generate() function.
     */
    DynamicPolicyData applyPolicy(DynamicPolicyData data);
}
