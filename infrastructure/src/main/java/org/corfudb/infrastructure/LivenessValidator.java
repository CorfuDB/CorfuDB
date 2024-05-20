package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Getter;
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
import org.corfudb.runtime.view.StreamsView.StreamId;
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

public class LivenessValidator {
    private final Map<TableName, LivenessMetadata> validateLivenessMap = new HashMap<>();
    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;
    private final Duration timeout;
    private final LivenessValidatorHelper livenessValidatorHelper;
    private final Logger log;

    private static final long LIVENESS_INIT_VALUE = -1;

    public LivenessValidator(CorfuRuntime corfuRuntime, CorfuStore corfuStore, Duration timeout) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = corfuStore;
        this.timeout = timeout;
        this.livenessValidatorHelper = new LivenessValidatorHelper();
        this.log = LoggerFactory.getLogger("compactor-leader");
    }

    @AllArgsConstructor
    @Getter
    private static class LivenessMetadata {
        private long heartbeat;
        private long streamTail;
        private Duration time;
    }

    @Getter
    private static class LivenessValidatorHelper {
        private long prevIdleCount = LIVENESS_INIT_VALUE;
        private Duration prevActiveTime = Duration.ofMillis(LIVENESS_INIT_VALUE);

        public void updateValues(long idleCount, Duration activeTime) {
            prevIdleCount = idleCount;
            prevActiveTime = activeTime;
        }

        public void clear() {
            prevIdleCount = LIVENESS_INIT_VALUE;
            prevActiveTime = Duration.ofMillis(LIVENESS_INIT_VALUE);
        }
    }

    public static enum Status {
        NONE,
        FINISH
    }

    public boolean isTableCheckpointActive(TableName table, Duration currentTime) {
        livenessValidatorHelper.clear();
        log.trace("Checking if table checkpoint is active...");
        if (validateLivenessMap.containsKey(table)) {
            LivenessMetadata previousStatus = validateLivenessMap.get(table);
            return isTailMovingForward(table, currentTime) || isHeartBeatMovingForward(table, currentTime) ||
                    currentTime.minus(previousStatus.getTime()).compareTo(timeout) <= 0;
        } else {
            validateLivenessMap.put(table, new LivenessMetadata(getHeartbeat(table), getCpStreamTail(table), currentTime));
        }
        return true;
    }

    private boolean isTailMovingForward(TableName table, Duration currentTime) {
        long cpStreamTail = getCpStreamTail(table);
        LivenessMetadata previousStatus = validateLivenessMap.get(table);
        if (previousStatus.getStreamTail() < cpStreamTail) {
            validateLivenessMap.put(table, new LivenessMetadata(previousStatus.getHeartbeat(),
                    cpStreamTail, currentTime));
            return true;
        }
        log.debug("Tail not moving forward for table: {}", table);
        return false;
    }

    private boolean isHeartBeatMovingForward(TableName table, Duration currentTime) {
        long syncHeartBeat = getHeartbeat(table);
        LivenessMetadata previousStatus = validateLivenessMap.get(table);
        if (previousStatus.getHeartbeat() < syncHeartBeat) {
            validateLivenessMap.put(table, new LivenessMetadata(syncHeartBeat,
                    previousStatus.getStreamTail(), currentTime));
            return true;
        }
        log.debug("HeartBeat not moving forward for table: {}", table);
        return false;
    }

    private long getHeartbeat(TableName table) {
        long syncHeartBeat = LIVENESS_INIT_VALUE;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            ActiveCPStreamMsg activeCPStreamMsg = (ActiveCPStreamMsg) txn.getRecord(
                    CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, table).getPayload();
            txn.commit();
            if (activeCPStreamMsg != null) {
                syncHeartBeat = activeCPStreamMsg.getSyncHeartbeat();
            }
        } catch (Exception e) {
            log.warn("Unable to acquire ActiveCPStreamMsg {}", table, e);
        }
        return syncHeartBeat;
    }

    @VisibleForTesting
    public long getCpStreamTail(TableName table) {
        FullyQualifiedTableName cpStreamName = FullyQualifiedTableName.build(table);
        UUID cpStreamId = StreamId.buildCkpStreamId(cpStreamName.toFqdn()).getId();
        return corfuRuntime
                .getSequencerView()
                .getStreamAddressSpace(new StreamAddressRange(cpStreamId, Address.MAX, Address.NON_ADDRESS))
                .getTail();
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

    public Status shouldChangeManagerStatus(Duration currentTime) {
        //Find the number of tables with IDLE status
        long idleCount = getIdleCount();
        if (livenessValidatorHelper.getPrevActiveTime().equals(Duration.ofMillis(LIVENESS_INIT_VALUE)) ||
                idleCount < livenessValidatorHelper.getPrevIdleCount()) {
            log.trace("Checkpointing in progress...");
            livenessValidatorHelper.updateValues(idleCount, currentTime);
            return Status.NONE;
        }

        Optional<CheckpointingStatus> managerStatus = Optional.empty();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            managerStatus = Optional.ofNullable((CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload());
            txn.commit();
            log.trace("ManagerStatus: {}", managerStatus.get().getStatus());
        } catch (Exception e) {
            log.warn("Unable to acquire Manager Status, ", e);
        }

        boolean isTimedOut = currentTime.minus(livenessValidatorHelper.getPrevActiveTime()).compareTo(timeout) > 0;

        if (idleCount == 0 || isTimedOut &&
                managerStatus.isPresent() && managerStatus.get().getStatus() == StatusType.STARTED) {
            return Status.FINISH;
        }
        return Status.NONE;
    }

    public void clearLivenessMap() {
        validateLivenessMap.clear();
    }

    public void clearLivenessValidator() {
        livenessValidatorHelper.clear();
    }
}
