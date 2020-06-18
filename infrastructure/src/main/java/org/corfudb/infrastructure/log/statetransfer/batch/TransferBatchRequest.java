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
 * and store the corresponding log data to the local stream log.
 */
@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
public class TransferBatchRequest {
    /**
     * A type of transfer batch. A type of DATA signals to the transfer processor that a batch needs
     * to be transferred, while the type of SEGMENT_INIT signals that the consecutive DATA batches
     * will belong to the same segment.
     */
    public enum TransferBatchType{
        DATA,
        SEGMENT_INIT
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
     * A type of a batch.
     */
    @Default
    private final TransferBatchType batchType = TransferBatchType.DATA;
}
