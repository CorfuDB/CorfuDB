package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class StaticPolicyData {
    private final List<Long> addresses;
    private final List<String> availableServers;
    private final int defaultBatchSize;
}
