package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy;

/**
 * An interface that static policies should implement to define the initial properties of a stream.
 */
@FunctionalInterface
public interface StaticPolicy {

    InitialBatchStream applyPolicy(StaticPolicyData data);
}
