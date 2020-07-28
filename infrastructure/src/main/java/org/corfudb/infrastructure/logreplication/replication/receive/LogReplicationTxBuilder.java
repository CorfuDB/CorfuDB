package org.corfudb.infrastructure.logreplication.replication.receive;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataVal;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.Address;

import java.util.UUID;

/**
 * Transaction builder used by Log Replication.
 * Provide the API to commit the log replication metadata along with data.
 */
@Slf4j
public class LogReplicationTxBuilder {

    LogReplicationMetadataManager metadataManager;

    @Getter
    TxBuilder txBuilder;

    @Getter
    LogReplicationMetadataVal.Builder metadataBuilder;

    @Getter
    CorfuStoreMetadata.Timestamp timestamp;

    public static LogReplicationTxBuilder getLogReplicationTxBuilder(LogReplicationMetadataManager metadataManager) {
        return new LogReplicationTxBuilder(metadataManager);
    }

    LogReplicationTxBuilder(LogReplicationMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
        this.timestamp = metadataManager.getTimestamp();
        this.txBuilder = metadataManager.getCorfuStore().tx(LogReplicationMetadataManager.getNAMESPACE());
        this.metadataBuilder = LogReplicationMetadataVal.newBuilder(metadataManager.queryPersistedMetadata());
    }

    /**
     * This API is used to append the metadata commit to the transaction that updates the shadow stream or real stream.
     *
     * @param metadataName
     * @param val
     */
    public void appendUpdate(LogReplicationMetadataManager.LogReplicationMetadataName metadataName, long val) {

        switch (metadataName) {
            case TOPOLOGY_CONFIG_ID:
                metadataBuilder.setTopologyConfigId(val);
                break;

            case LAST_SNAPSHOT_STARTED:
                metadataBuilder.setSnapshotStartTimestamp(val).
                        setSnapshotMessageReceivedSeqNum(Address.NON_ADDRESS).
                        setSnapshotMessageAppliedSeqNum(Address.NON_ADDRESS);
                break;

            case LAST_SNAPSHOT_TRANSFERRED:
                metadataBuilder.setSnapshotTransferredTimestamp(val);
                break;

            case LAST_SNAPSHOT_APPLIED:
                metadataBuilder.setSnapshotAppliedTimestamp(val);
                break;

            case LAST_SNAPSHOT_MSG_RECEIVED_SEQ_NUM:
                metadataBuilder.setSnapshotMessageReceivedSeqNum(val);
                break;

            case LAST_SNAPSHOT_MSG_APPLIED_SEQ_NUM:
                metadataBuilder.setSnapshotMessageAppliedSeqNum(val);
                break;

            case LAST_LOG_PROCESSED:
                metadataBuilder.setLastLogEntryProcessedTimestamp(val);
                break;

            case CURRENT_SNAPSHOT_CYCLE_ID:
                metadataBuilder.setCurrentSnapshotCycleId(val);
                break;

            case CURRENT_CYCLE_MIN_SHADOW_STREAM_TS:
                metadataBuilder.setMinShadowStreamTimestamp(val);
                break;

            default:
                log.warn("there is no metadata name for {}", metadataName);
                return;
        }
    }

    public void logUpdate(UUID streamId, SMREntry updateEntry) {
        txBuilder.logUpdate(streamId, updateEntry);
    }

    public void commit() {
        if (metadataBuilder != null) {
            LogReplicationMetadataVal metadataVal = metadataBuilder.build();
            txBuilder.update(metadataManager.getMetadataTableName(), metadataManager.getCurrentMetadataKey(), metadataVal, null);
        } else {
            log.error("There is no metadata update along the data update. This could lead to corrupted data if two leaders update the data at the same time");
        }
        txBuilder.commit(timestamp);
    }

    /**
     * Update the metadata val. The commit(LogReplicationMetadataVal) api and appendUpdate api could not be used in the same transaction,
     * as this API will overwrite any updates of metadata in the appendUpdate API.
     * @param metadataVal
     */
    public void commit(LogReplicationMetadataVal metadataVal) {
        if (metadataBuilder != null) {
            log.error("There are some mix use of appendUpdate with this commit(LogReplicationMetadataVal) API");
        }
        txBuilder.update(metadataManager.getMetadataTableName(), metadataManager.getCurrentMetadataKey(), metadataVal, null);
        txBuilder.commit();
    }

    /**
     * Given topologyConfigId and version number, build a metadataVal with log replication status with init values.
     *
     * @param topologyConfigId
     * @param version
     * @return
     */
    public static LogReplicationMetadataVal buildMetadataValInstance(long topologyConfigId, String version) {
        LogReplicationMetadataVal metadataVal = LogReplicationMetadataVal.newBuilder().
                setTopologyConfigId(topologyConfigId).
                setVersion(version).
                setSnapshotStartTimestamp(Address.NON_ADDRESS).
                setSnapshotTransferredTimestamp(Address.NON_ADDRESS).
                setSnapshotAppliedTimestamp(Address.NON_ADDRESS).
                setSnapshotMessageReceivedSeqNum(Address.NON_ADDRESS).
                setSnapshotMessageAppliedSeqNum(Address.NON_ADDRESS).
                setLastLogEntryProcessedTimestamp(Address.NON_ADDRESS).build();
        return metadataVal;
    }

}
