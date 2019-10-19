package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import lombok.Builder;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.DynamicPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.IdentityPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.errorpolicy.RemoveServersWithRoundRobin;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.RoundRobinPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.StaticPolicy;

import static lombok.Builder.*;

@Builder
@Getter
public class PolicyStreamProcessorData {

    @Default
    private final StaticPolicy initialDistributionPolicy = new RoundRobinPolicy();

    @Default
    private final DynamicPolicy dynamicDistributionPolicy = new IdentityPolicy();

    @Default
    private final DynamicPolicy batchProcessorFailureHandlingPolicy = new RemoveServersWithRoundRobin();
}
