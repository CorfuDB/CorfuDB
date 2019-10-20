package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy;

/**
 * A dynamic policy that does not transform a tail of a stream.
 */
public class IdentityPolicy implements DynamicPolicy {
    @Override
    public DynamicPolicyData applyPolicy(DynamicPolicyData data) {
        return data;
    }
}
