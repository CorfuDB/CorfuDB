package org.corfudb.infrastructure.compaction;

public class TriggerTask implements Runnable{
    
    private TriggerPolicy policy;

    public TriggerTask() {
        policy = new HybridTriggerPolicy();
    }

    @Override
    public void run() {

    }
}
