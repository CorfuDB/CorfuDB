package org.corfudb.infrastructure.compaction;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class HybridTriggerPolicy implements TriggerPolicy{
    @Override
    public boolean shouldTrigger() {
        return false;
    }

    @Override
    public void reset() {

    }

    @Override
    public TimeUnit getInterval() {
        return null;
    }

    @Override
    public SafeSnapshot getSafeSnapshot() {
        return null;
    }
}
