package org.corfudb.infrastructure.log.statetransfer;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CommittedTransferData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessorData;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessorData;

import java.util.List;

@Getter
@Builder
public class StateTransferConfig {

    @NonNull
    private final List<Long> unknownAddresses;

    @NonNull
    private final CommittedTransferData committedTransferData;

    @NonNull
    private final StateTransferBatchProcessorData batchProcessorData;

    @Default
    private final int batchSize = 10;

    @Default
    private final PolicyStreamProcessorData policyStreamProcessorData =
            PolicyStreamProcessorData.builder().build();


}
