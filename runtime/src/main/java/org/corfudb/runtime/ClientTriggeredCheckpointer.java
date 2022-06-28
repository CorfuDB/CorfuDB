package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientTriggeredCheckpointer extends DistributedCheckpointer {

    private final CheckpointerBuilder checkpointerBuilder;

    public ClientTriggeredCheckpointer(CheckpointerBuilder checkpointerBuilder) {
        super(checkpointerBuilder.corfuRuntime, checkpointerBuilder.clientName);
        this.checkpointerBuilder = checkpointerBuilder;
    }

    @Override
    public void checkpointTables() {
        if (!openCompactorMetadataTables(getCorfuStore())) {
            return;
        }
        if (DistributedCheckpointerHelper.isUpgrade(getCorfuStore()) ||
                !DistributedCheckpointerHelper.hasCompactionStarted(getCorfuStore())) {
            log.info("Compaction hasn't started...");
            return;
        }
        int count = checkpointOpenedTables();
        log.info("{}: Finished checkpointing {} opened tables", checkpointerBuilder.clientName, count);
    }
}
