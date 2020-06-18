package org.corfudb.infrastructure.log.statetransfer.batch;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.List;

/**
 * A transfer batch request for a specific node to read the addresses from.
 */
@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class TransferBatchRequestForNode {

    /**
     * A batch of addresses, small enough to get transferred within one rpc call.
     */
    @NonNull
    private final List<Long> addresses;

    /**
     * A specific destination endpoint to read the addresses from.
     */
    @NonNull
    private final String destinationNode;
}
