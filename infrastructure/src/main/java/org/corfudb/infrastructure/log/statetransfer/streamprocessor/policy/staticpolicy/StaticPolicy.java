package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy;

import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import java.util.stream.Stream;

/**
 * An interface that static policies should implement to define the initial properties of a stream.
 */
@FunctionalInterface
public interface StaticPolicy {

    Stream<Batch> applyPolicy(StaticPolicyData data);
}
