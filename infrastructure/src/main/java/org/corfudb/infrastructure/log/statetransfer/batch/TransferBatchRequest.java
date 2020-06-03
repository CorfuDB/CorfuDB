package org.corfudb.infrastructure.log.statetransfer.batch;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Optional;

/**
 * A request to transfer a batch of addresses from a particular destination node
 * (if {@link #destinationNodes} is present) or via a replication protocol otherwise,
 * and store the corresponding log data to the local stream log. {@link #transferBatchType} of
 * DATA means that the batch is meant for a transfer by a particular StateTransferBatchProcessor,
 * while a type of POISON_PILL is meant to signal the end of the transfer batch request stream for a
 * parallel transfer.
 */
@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
public class TransferBatchRequest {
    public enum TransferBatchType {
        DATA,
        POISON_PILL
    }

    /**
     * A batch of addresses, small enough to get transferred within one rpc call.
     */
    @Default
    private final List<Long> addresses = ImmutableList.of();
    /**
     * Potential destination endpoints. This field is optional because the log data
     * can be read using the cluster consistency protocol rather than from a specific destination
     * if the part of the log we want to transfer is inconsistent.
     */
    @Default
    private final Optional<ImmutableList<String>> destinationNodes = Optional.empty();

    /**
     * A type of transfer batch request. For the parallel transfer the type of POISON_PILL
     * will signal the end of the stream and the initiation of the graceful shutdown. Type
     * of DATA means that the addresses should be transferred using
     * the appropriate StateTransferBatchProcessor.
     */
    @Default
    private final TransferBatchType transferBatchType = TransferBatchType.DATA;
}
