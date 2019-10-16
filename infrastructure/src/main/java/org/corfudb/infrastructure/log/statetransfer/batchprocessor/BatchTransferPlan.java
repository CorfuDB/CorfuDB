package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class BatchTransferPlan {
    private final List<Long> transferAddresses;
}