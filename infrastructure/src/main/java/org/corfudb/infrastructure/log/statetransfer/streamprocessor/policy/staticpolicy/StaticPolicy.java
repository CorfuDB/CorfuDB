package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy;

import org.corfudb.infrastructure.log.statetransfer.batch.Batch;

import java.util.Optional;
import java.util.stream.Stream;

@FunctionalInterface
public interface StaticPolicy {

    Stream<Optional<Batch>> applyPolicy(StaticPolicyData data);
}
