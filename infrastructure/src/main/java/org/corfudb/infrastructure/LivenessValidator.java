package org.corfudb.infrastructure;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class LivenessValidator {
    private final Map<TableName, LivenessMetadata> validateLivenessMap = new HashMap<>();
    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;
    private final Duration timeout;
    private final LivenessValidatorHelper livenessValidatorHelper;

    private static final long LIVENESS_INIT_VALUE = -1;


    public LivenessValidator(CorfuRuntime corfuRuntime, CorfuStore corfuStore, Duration timeout) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = corfuStore;
        this.timeout = timeout;
        this.livenessValidatorHelper = new LivenessValidatorHelper();
    }

    @AllArgsConstructor
    @Getter
    private class LivenessMetadata {
        private long heartbeat;
        private long streamTail;
        private Duration time;
    }

    @Getter
    private class LivenessValidatorHelper {
        private long prevIdleCount = LIVENESS_INIT_VALUE;
        private Duration prevActiveTime = Duration.ofMillis(LIVENESS_INIT_VALUE);

        public void updateValues(long idleCount, Duration activeTime) {
            prevIdleCount = idleCount;
            prevActiveTime = activeTime;
        }

        public void clear() {
            prevIdleCount = LIVENESS_INIT_VALUE;
            prevActiveTime = Duration.ofMillis(LIVENESS_INIT_VALUE);
            ;
        }
    }

    public static enum StatusToChange {
        NONE,
        STARTED_ALL,
        FINISH
    }

    public boolean isTableCheckpointActive(TableName table, Duration currentTime) {
        livenessValidatorHelper.clear();

        String streamName = TableRegistry.getFullyQualifiedTableName(table.getNamespace(), table.getTableName());
        UUID streamId = CorfuRuntime.getCheckpointStreamIdFromName(streamName);
        long currentStreamTail = corfuRuntime.getSequencerView()
                .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS)).getTail();
        long syncHeartBeat = LIVENESS_INIT_VALUE;

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            ActiveCPStreamMsg activeCPStreamMsg = (ActiveCPStreamMsg) txn.getRecord(
                    CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, table).getPayload();
            txn.commit();
            if (activeCPStreamMsg == null) {
                return true;
            }
            syncHeartBeat = activeCPStreamMsg.getSyncHeartbeat();
        } catch (Exception e) {
            log.warn("Unable to acquire ActiveCPStreamMsg {}", table, e);
        }

        if (validateLivenessMap.containsKey(table)) {
            LivenessMetadata previousStatus = validateLivenessMap.get(table);
            if (previousStatus.getStreamTail() < currentStreamTail) {
                validateLivenessMap.put(table, new LivenessMetadata(previousStatus.getHeartbeat(),
                        currentStreamTail, currentTime));
            } else if (previousStatus.getHeartbeat() < syncHeartBeat) {
                validateLivenessMap.put(table, new LivenessMetadata(syncHeartBeat,
                        previousStatus.getStreamTail(), currentTime));
            } else if (currentTime.minus(previousStatus.getTime()).compareTo(timeout) > 0) {
                return false;
            }
        } else {
            validateLivenessMap.put(table, new LivenessMetadata(syncHeartBeat, currentStreamTail, currentTime));
        }
        return true;
    }

    private int getIdleCount() {
        int idleCount = 0;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            idleCount = txn.executeQuery(CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, entry -> (
                    (CheckpointingStatus) entry.getPayload()).getStatus() == StatusType.IDLE).size();
            log.trace("Number of idle tables: {}", idleCount);
            txn.commit();
        }
        return idleCount;
    }

    public StatusToChange shouldChangeManagerStatus(Duration currentTime) {
        //Find the number of tables with IDLE status
        long idleCount = getIdleCount();
        if (livenessValidatorHelper.getPrevActiveTime().equals(Duration.ofMillis(LIVENESS_INIT_VALUE)) ||
                idleCount < livenessValidatorHelper.getPrevIdleCount()) {
            log.trace("Checkpointing in progress...");
            livenessValidatorHelper.updateValues(idleCount, currentTime);
            return StatusToChange.NONE;
        }

        Optional<CheckpointingStatus> managerStatus = Optional.empty();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            managerStatus = Optional.ofNullable((CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload());
            txn.commit();
        } catch (Exception e) {
            log.warn("Unable to acquire Manager Status");
        }

        boolean isTimedOut = currentTime.minus(livenessValidatorHelper.getPrevActiveTime()).compareTo(timeout) > 0;

        if (idleCount == 0 || isTimedOut &&
                managerStatus.isPresent() && managerStatus.get().getStatus() == StatusType.STARTED_ALL) {
            return StatusToChange.FINISH;
        } else if (isTimedOut && managerStatus.isPresent() && managerStatus.get().getStatus() == StatusType.STARTED) {
            log.info("No active client checkpointers available...");
            return StatusToChange.STARTED_ALL;
        }
        return StatusToChange.NONE;
    }

    public void clearLivenessMap() {
        validateLivenessMap.clear();
    }

    public void clearLivenessValidator() {
        livenessValidatorHelper.clear();
    }
}
