package org.corfudb.infrastructure.log.statetransfer.batch;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static lombok.Builder.Default;

/**
 * A result of a read that contains:
 * - {@link #data} - a piece of data read from the cluster (or from a specific node).
 * - {@link #failedAddresses} - a list of addresses, for which the data was not read.
 * - {@link #destination } - an optional destination server where the corresponding data is present,
 * if not read via a replication protocol.
 * - {@link #status} - a status describing a result of a read -- success or failure.
 */
@Getter
@ToString
@Builder
public class ReadBatch {
    public enum ReadStatus {
        SUCCEEDED,
        FAILED
    }

    /**
     * A batch of log data read from the cluster (or from a specific node).
     */
    @Default
    private final List<LogData> data = ImmutableList.of();

    /**
     * Addresses for which the data was not read.
     */
    @Default
    private final List<Long> failedAddresses = ImmutableList.of();
    /**
     * A destination endpoint. This field is optional because the log data
     * can be read using the cluster consistency protocol rather than from a specific destination.
     */
    @Default
    private final Optional<String> destination = Optional.empty();

    /**
     * A status of success or failure after read was performed.
     */
    @Default
    private final ReadStatus status = ReadStatus.SUCCEEDED;

    /**
     * Creates a transferBatchRequest from {@link #failedAddresses} and {@link #destination}.
     *
     * @return An instance of transferBatchRequest.
     */
    public TransferBatchRequest createRequest() {
        return new TransferBatchRequest(failedAddresses, destination);
    }

    /**
     * Get addresses of the successfully transferred records.
     * @return Addresses.
     */
    public List<Long> getAddresses() {
        return data.stream()
                .map(IMetadata::getGlobalAddress)
                .collect(Collectors.toList());
    }


}
