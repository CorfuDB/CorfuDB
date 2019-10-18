package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy;

import org.corfudb.infrastructure.log.statetransfer.batch.Batch;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class IdentityPolicy implements DynamicPolicy {
    @Override
    public DynamicPolicyData applyPolicy(DynamicPolicyData data) {
        return data;
    }
}
