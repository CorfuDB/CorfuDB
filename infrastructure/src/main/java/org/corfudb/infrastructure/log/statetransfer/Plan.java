package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.StaticPolicyData;

/**
 * A state transfer plan, used to execute a state transfer.
 */
@Getter
@AllArgsConstructor
public class Plan {

    @Getter
    @Builder
    public static class Bundle {
        @NonNull
        private final StaticPolicyData data;
        @NonNull
        private final PolicyStreamProcessor processor;
    }

    /**
     * A list of pairs of a stream processor and a static policy data
     * needed to execute a transfer.
     */
    @NonNull
    private final ImmutableList<Bundle> bundles;

}
