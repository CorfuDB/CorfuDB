package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy;

/**
 * An interface that dynamic policies should implement to transform the tail of a stream.
 */
@FunctionalInterface
public interface DynamicPolicy {

    /**
     * Based on the current dynamic policy data
     * apply the function to get the new dynamic policy data.
     * @param data A dynamic policy data.
     * @return A new dynamic policy data.
     */
    DynamicPolicyData applyPolicy(DynamicPolicyData data);
}
