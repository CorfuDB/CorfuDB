package org.corfudb.infrastructure.log.statetransfer.batch;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class BatchResultData {
    private final long highestTransferredAddress;
    private final String destinationServer;
}
