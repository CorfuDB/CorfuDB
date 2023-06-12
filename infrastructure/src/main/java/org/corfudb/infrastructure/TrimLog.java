package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

public class TrimLog {
    private final Logger log;

    TrimLog() {
        this.log = LoggerFactory.getLogger("compactor-leader");
    }

    private Optional<Long> getTrimAddress(CorfuStore corfuStore) {
        Optional<Long> trimAddress = Optional.empty();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
            if (managerStatus.getStatus() == CheckpointingStatus.StatusType.COMPLETED) {
                RpcCommon.TokenMsg trimToken = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                        CompactorMetadataTables.MIN_CHECKPOINT).getPayload();
                trimAddress = Optional.of(trimToken.getSequence());
            } else {
                log.warn("Skip trimming since last checkpointing cycle did not complete successfully");
            }
            txn.commit();
        } catch (Exception e) {
            log.warn("Unable to acquire the trim token");
        }
        return trimAddress;
    }

    /**
     * Perform log-trimming on CorfuDB
     */
    public void invokePrefixTrim(CorfuRuntime corfuRuntime, CorfuStore corfuStore) {
        Optional<Long> trimAddress = getTrimAddress(corfuStore);
        if (!trimAddress.isPresent()) {
            return;
        }
        // Measure time spent on trimming.
        final long startTime = System.nanoTime();
        corfuRuntime.getAddressSpaceView().prefixTrim(
                new Token(0, trimAddress.get()));
        corfuRuntime.getAddressSpaceView().gc();
        final long endTime = System.nanoTime();

        log.info("Trim completed, elapsed({}s), log address up to {}.",
                TimeUnit.NANOSECONDS.toSeconds(endTime - startTime), trimAddress.get());
    }
}
