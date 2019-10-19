package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.AddressSpaceView;

import java.util.Map;

@Getter
@AllArgsConstructor
public class StateTransferBatchProcessorData {
    private final StreamLog streamLog;
    private final AddressSpaceView addressSpaceView;
    private final Map<String, LogUnitClient> clientMap;
}
