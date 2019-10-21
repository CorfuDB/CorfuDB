package org.corfudb.infrastructure.log.statetransfer;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CommittedTransferData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessorData;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessorData;

import java.util.List;
import java.util.Optional;

/**
 * A config needed to execute an instance of a state transfer.
 */
@Getter
@Builder
@EqualsAndHashCode(exclude = {"batchProcessorData", "policyStreamProcessorData"})
public class StateTransferConfig {

    /**
     * Addresses to transfer within this state transfer.
     */
    @NonNull
    private final List<Long> unknownAddresses;

    /**
     * An optional committed transfer data to perform state transfer more optimally.
     */
    @Default
    private final Optional<CommittedTransferData> committedTransferData = Optional.empty();

    /**
     * A piece of data that batch processor needs to perform a batch transfer
     */
    @NonNull
    private final StateTransferBatchProcessorData batchProcessorData;

    /**
     * A size of every transfer batch, small enough to fit within one rpc call.
     */
    @NonNull
    private final int batchSize;

    /**
     * A set of predefined policies needed to execute the state transfer.
     */
    @Default

    private final PolicyStreamProcessorData policyStreamProcessorData =
            PolicyStreamProcessorData.builder().build();


}
