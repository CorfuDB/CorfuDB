package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchTransferPlan;

import java.util.List;

@Getter
public class ProtocolTransferPlan extends BatchTransferPlan {
    public ProtocolTransferPlan(List<Long> transferAddresses) {
        super(transferAddresses);
    }
}