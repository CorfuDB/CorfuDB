package org.corfudb.infrastructure;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCompactor;

public class InvokeCheckpointingMock implements IInvokeCheckpointing {

    private final DistributedCompactor distributedCompactor;
    private boolean isInvoked;

    public InvokeCheckpointingMock(CorfuRuntime runtime, CorfuRuntime cpRuntime) {
        this.distributedCompactor =
                new DistributedCompactor(runtime, cpRuntime, null);
    }

    @Override
    public void invokeCheckpointing() {
        isInvoked = true;
        distributedCompactor.startCheckpointing();
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public boolean isInvoked() {
        return isInvoked;
    }

    @Override
    public void shutdown() {
        //Not invoked
    }
}
