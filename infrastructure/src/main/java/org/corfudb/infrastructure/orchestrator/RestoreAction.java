package org.corfudb.infrastructure.orchestrator;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.Nonnull;

@Slf4j
/**
 * A workflow action that invokes restore redundancy and merge segment action.
 */
public abstract class RestoreAction extends Action {

    public void impl(@Nonnull CorfuRuntime runtime) throws Exception{
        throw new IllegalStateException();
    }

    public abstract void impl(@Nonnull CorfuRuntime runtime, @NonNull StreamLog streamLog) throws Exception;

    @Nonnull
    public void execute(@Nonnull CorfuRuntime runtime, @NonNull StreamLog streamLog, int numRetry) {
        for (int x = 0; x < numRetry; x++) {
            try {
                changeStatus(ActionStatus.STARTED);
                impl(runtime, streamLog);
                changeStatus(ActionStatus.COMPLETED);
                return;
            } catch (Exception e) {
                log.error("execute: Error executing action {} on retry {}. Invalidating layout.",
                        getName(), x, e);
                changeStatus(ActionStatus.ERROR);
                runtime.invalidateLayout();
            }
        }
    }
}
