package org.corfudb.infrastructure.logreplication;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationWriterMetadata.LogReplicationMetadataKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationWriterMetadata.LogReplicationMetadataVal;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Address;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

/**
 * The table persisted at the replication writer side.
 * It records the reader cluster's snapshot timestamp  and last log entry's timestamp, it has received and processed.
 */
@Slf4j
public class PersistedWriterMetadata {
    private static final String namespace = "CORFU_SYSTEM";
    private static final String TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";
    private static final int NUM_RETRY_WRITE = 3;
    String metadataTableName;

    private CorfuStore corfuStore;

    Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> metadataTable;

    CorfuRuntime runtime;

    public PersistedWriterMetadata(CorfuRuntime rt, long siteEpoch, UUID primary, UUID dst) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        this.runtime = rt;
        metadataTableName = getPersistedWriterMetadataTableName(primary, dst);
        metadataTable = this.corfuStore.openTable(namespace,
                metadataTableName,
                LogReplicationMetadataKey.class,
                LogReplicationMetadataVal.class,
                null,
                TableOptions.builder().build());

        setupEpoch(siteEpoch);
    }


    long query(CorfuStoreMetadata.Timestamp timestamp, PersistedWriterMetadataType key) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        CorfuRecord record = corfuStore.query(namespace).getRecord(metadataTableName, timestamp, txKey);
        LogReplicationMetadataVal metadataVal = null;
        long val = -1;

        if (record != null) {
            metadataVal = (LogReplicationMetadataVal)record.getPayload();
        }

        if (metadataVal != null) {
            val = metadataVal.getVal();
        }

        return val;
    }

    void appendUpdate(TxBuilder txBuilder, PersistedWriterMetadataType key, long val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(val).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    /**
     *
     * @param epoch
     */
    public void setupEpoch(long epoch) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();

        long persistEpoch = query(timestamp, PersistedWriterMetadataType.SiteEpoch);

        if (epoch <= persistEpoch)
            return;

        TxBuilder txBuilder = corfuStore.tx(namespace);

        for (PersistedWriterMetadataType key : PersistedWriterMetadataType.values()) {
            long val = Address.NON_ADDRESS;
            if (key == PersistedWriterMetadataType.SiteEpoch) {
                val = epoch;
            }
            appendUpdate(txBuilder, key, val);
         }

        txBuilder.commit(timestamp);
    }

    public static String getPersistedWriterMetadataTableName(UUID primarySite, UUID dst) {
        return TABLE_PREFIX_NAME + primarySite.toString() + "-to-" + dst.toString();
    }


    /**
     * If the current ts is smaller than the persisted ts, ingore it.
     * If the current ts is equal or bigger than the current ts, will
     * bump the epic number, and also update the ts, cleanup the persistentQueue for prepare
     * snapshot transfer.
     *
     * @param ts
     * @return
     */
    public long setSrcBaseSnapshotStartnew(long epoch, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        return 0;
    }


    public long setSrcBaseSnapshotStart(long epoch, long ts) {
        long persistedEpic = 0;
        long persistedTs = 0;
        int retry = 0;
        boolean doRetry = true;
        while (retry++ < NUM_RETRY_WRITE && doRetry) {
            try {
                runtime.getObjectsView().TXBegin();
                persistedEpic = writerMetaDataTable.get(PersistedWriterMetadataType.SiteEpoch.getVal());
                persistedTs = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());

                if (epoch == persistedEpic && ts >= persistedTs) {
                    writerMetaDataTable.put(PersistedWriterMetadataType.SiteEpoch.getVal(), persistedEpic);
                    writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapStart.getVal(), ts);
                    writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapSeqNum.getVal(), Address.NON_ADDRESS);
                    //TODO:  clean persistentQue if no AR
                    log.info("Update the snapshot epic {} lastSnapStart {} lastSnapSeqNum {} ",
                            persistedTs, ts, Address.NON_ADDRESS);
                } else {
                    log.warn("the current snapStart is not larger than the persisted snapStart {}, skip the update ",
                            ts, persistedTs);
                }
                doRetry = false;
            } catch (TransactionAbortedException e) {
                log.debug("Caught transaction aborted exception {}", e.getStackTrace());
                log.warn("While trying to update lastSnapStart value to {}, aborted with retry {}", ts, retry);
            } finally {
                runtime.getObjectsView().TXEnd();
            }
        }

        return writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());
    }

    public void setSrcBaseSnapshotStart(long ts) {
        writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapStart.getVal(), ts);
    }

    /**
     * If the persistent data show it is my epic and my snapshot value, will update the
     * snapshot timestamp and the lastlog processed timestamp
     */
    public void setSrcBaseSnapshotDone(LogReplicationEntry entry) {
        try {
            runtime.getObjectsView().TXBegin();
            long epoch = writerMetaDataTable.get(PersistedWriterMetadataType.SiteEpoch.getVal());
            long ts = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());
            if (epoch == entry.getMetadata().getSiteEpoch() && ts == entry.getMetadata().getSnapshotTimestamp()) {
                writerMetaDataTable.put(PersistedWriterMetadataType.SiteEpoch.getVal(), epoch);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapStart.getVal(), ts);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapTransferDone.getVal(), ts);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapApplyDone.getVal(), ts);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastLogProcessed.getVal(), ts);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapSeqNum.getVal(), Address.NON_ADDRESS);
                log.info("update lastSnapTransferDone {} ", ts);
            } else {
                log.warn("skip update lastSnapTransferDone as Epic curent {} != persist {} or BaseSnapStart: current {} != persist {}",
                        entry.getMetadata().getSiteEpoch(), epoch, entry.getMetadata().getSnapshotTimestamp(), ts);
            }
        } catch (TransactionAbortedException e) {
            log.warn("Transaction is aborted. The snapshot has been updated by someone else");
        } finally {
            runtime.getObjectsView().TXEnd();
        }
    }

    public long getLastSnapStartTimestamp() {
        return writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());
    }

    public long getLastSrcBaseSnapshotTimestamp() {
        return writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapApplyDone.getVal());
    }

    /**
     * This call should be done in a transaction while applying a log entry message.
     * Only update the lastLogProcesed timestamp
     * @param ts
     */
    public void setLastProcessedLogTimestamp(long ts) {
        writerMetaDataTable.put(PersistedWriterMetadataType.LastLogProcessed.getVal(), ts);
    }

    public long getLastProcessedLogTimestamp() {
        return writerMetaDataTable.get(PersistedWriterMetadataType.LastLogProcessed.getVal());
    }

    /**
     * This call should be done in a transaction after a transfer done and before apply the snapshot.
     * @param ts
     */
    public void setLastSnapTransferDoneTimestamp(long ts) {
        writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapTransferDone.getVal(), ts);
    }

    public long getLastSnapTransferDoneTimestamp() {
        return writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapTransferDone.getVal());
    }

    /**
     * This should be called in a transaction context while applying a snapshot replication entry.
     * @param ts
     */
    public void setLastSnapSeqNum(long ts) {
        writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapSeqNum.getVal(), ts);
    }

    public long getLastSnapSeqNum() {
        return writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapSeqNum.getVal());
    }

    public void setSiteEpoch(long ts) {
        writerMetaDataTable.put(PersistedWriterMetadataType.SiteEpoch.getVal(), ts);
    }

    public long getSiteEpoch() {
        return writerMetaDataTable.get(PersistedWriterMetadataType.SiteEpoch.getVal());
    }

    public enum PersistedWriterMetadataType {
        SiteEpoch("SiteEpic"),
        LastSnapStart("lastSnapStart"),
        LastSnapTransferDone("lastSnapTransferDone"),
        LastSnapApplyDone("lastSnapApplied"),
        LastSnapSeqNum("lastSnapSeqNum"),
        LastLogProcessed("lastLogProcessed");

        @Getter
        String val;
        PersistedWriterMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
