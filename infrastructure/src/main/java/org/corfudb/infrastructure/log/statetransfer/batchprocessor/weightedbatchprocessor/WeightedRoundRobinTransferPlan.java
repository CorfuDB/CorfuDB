package org.corfudb.infrastructure.log.statetransfer.batchprocessor.weightedbatchprocessor;

import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchTransferPlan;
import java.util.List;

@Getter
public class WeightedRoundRobinTransferPlan extends BatchTransferPlan {

    private final List<String> exclusiveEndpoints;

    public WeightedRoundRobinTransferPlan(List<Long> transferAddresses, List<String> exclusiveEndpoints) {
        super(transferAddresses);
        this.exclusiveEndpoints = exclusiveEndpoints;
    }
}