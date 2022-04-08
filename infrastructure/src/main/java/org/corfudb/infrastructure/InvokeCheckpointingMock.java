package org.corfudb.infrastructure;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCompactor;

public class InvokeCheckpointingMock implements IInvokeCheckpointing {

    private final DistributedCompactor distributedCompactor;

    public InvokeCheckpointingMock(CorfuRuntime runtime, CorfuRuntime cpRuntime) {
        this.distributedCompactor =
                new DistributedCompactor(runtime, cpRuntime, null);
    }

    @Override
    public void invokeCheckpointing() {
        distributedCompactor.startCheckpointing();
    }

    @Override
    public void shutdown() {
    }
}
