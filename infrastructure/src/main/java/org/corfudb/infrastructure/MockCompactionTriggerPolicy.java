package org.corfudb.infrastructure;

import lombok.Setter;
import org.corfudb.runtime.CorfuRuntime;

public class MockCompactionTriggerPolicy implements ICompactionTriggerPolicy{

    @Setter
    private boolean shouldTrigger;

    @Override
    public boolean shouldTrigger(long interval) {
        if (shouldTrigger) {
            shouldTrigger = false;
            return true;
        }
        return false;
    }

    @Override
    public void setCorfuRuntime(CorfuRuntime corfuRuntime) {

    }
}
