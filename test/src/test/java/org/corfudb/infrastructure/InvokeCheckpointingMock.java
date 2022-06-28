package org.corfudb.infrastructure;

import org.corfudb.runtime.CheckpointerBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointer;
import org.corfudb.runtime.ServerTriggeredCheckpointer;

import java.util.Optional;

public class InvokeCheckpointingMock implements InvokeCheckpointing {

    private final DistributedCheckpointer distributedCheckpointer;
    private boolean isInvoked;

    public InvokeCheckpointingMock(CorfuRuntime runtime, CorfuRuntime cpRuntime) {
        this.distributedCheckpointer =
                new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                        .corfuRuntime(runtime)
                        .cpRuntime(Optional.of(cpRuntime))
                        .persistedCacheRoot(Optional.empty())
                        .isClient(false)
                        .build());
    }

    @Override
    public void invokeCheckpointing() {
        isInvoked = true;
        distributedCheckpointer.checkpointTables();
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
