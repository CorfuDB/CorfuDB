package org.corfudb.infrastructure.logreplication;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.utils.LogReplicationWriterMetadata.LogReplicationMetadataKey;
import org.corfudb.utils.LogReplicationWriterMetadata.LogReplicationMetadataVal;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
/**
 * The table persisted at the replication writer side.
 * It records the reader cluster's snapshot timestamp  and last log entry's timestamp, it has received and processed.
 */
@Slf4j
public class NewPersistedWriterMetadata {
    private static final String namespace = "CORFU_SYSTEM";

    // Contains the max epoch. This table has only one key.
    private static final String maxEpochTableName = "CORFU-REPLICATION-MAX-EPOCH";

    // For each epoch, it has its own metadata-table with name: corfu-replication-writer-epoch
    private static final String TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";

    private CorfuStore corfuStore;
    Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> maxEpochTable;

    CorfuRuntime runtime;

    long maxEpoch;
    /**
     * Open the table that contains the maxEpoch and the current maxEpoch's metadata table.
     *
     * @param rt
     * @param primary
     * @param dst
     * @param epoch
     */
    public NewPersistedWriterMetadata(CorfuRuntime rt, UUID primary, UUID dst, long epoch) {
        this.runtime = rt;
        this.corfuStore = new CorfuStore(runtime);

        try {
            maxEpochTable = this.corfuStore.openTable(namespace,
                    maxEpochTableName,
                    LogReplicationMetadataKey.class,
                    LogReplicationMetadataVal.class,
                    null,
                    TableOptions.builder().build());
            maxEpoch = -1;
            updateEpoAndOpenTable(epoch);
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
            log.warn("Caught an exception ", e);
        }
    }

    public static String getTableName(long epoch) {
        return TABLE_PREFIX_NAME + epoch;
    }

    public long getMaxEpoch() {
        LogReplicationMetadataKey epochKey = LogReplicationMetadataKey.newBuilder().setKey(maxEpochTableName).build();
        CorfuRecord<LogReplicationMetadataVal, LogReplicationMetadataVal> record = maxEpochTable.get(epochKey);
        if (record == null) {
            return -1;
        }

        return record.getPayload().getVal();
    }

    /**
     * If the current epoch is higher than the current max epoch
     * 1. create a table for this epoch
     * 2. update the maxEpoch value.
     *
     * @param epoch
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public synchronized void updateEpoAndOpenTable(long epoch) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        long newEpoch = Math.max(epoch, getMaxEpoch());

        if (newEpoch < maxEpoch)
            return;

        maxEpoch = newEpoch;
        try {
            corfuStore.openTable(namespace,
                    getTableName(maxEpoch),
                    LogReplicationMetadataKey.class,
                    LogReplicationMetadataVal.class,
                    null,
                    TableOptions.builder().build());

            updateMaxEpoch(maxEpoch);
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
            log.warn("Caught an exception ", e);
            throw e;
        }
    }

    private void updateMaxEpoch(long epoch) {
        LogReplicationMetadataKey epochKey = LogReplicationMetadataKey.newBuilder().setKey(maxEpochTableName).build();
        LogReplicationMetadataVal epochVal = LogReplicationMetadataVal.newBuilder().setVal(epoch).build();

        TxBuilder txBuilder = corfuStore.tx(namespace);
        if (epoch > getMaxEpoch()) {
            txBuilder.update(maxEpochTableName,
                    epochKey,
                    epochVal,
                    null);
        }
        txBuilder.commit();
    }

    /**
     * update the epoch to the current epoch, and corresponding key, value pair in the current epochTable.
     *
     * @param epoch should be the same as the currentMAX. The update Epoch is for fencing the transactions
     *              that update the metadata at the same time.
     * @param keys the list of keys to be updated
     * @param vals the list values corresponding to the key.
     */
    private void updateEpochAndKey(long epoch, List<String> keys, List<Long> vals) {
        LogReplicationMetadataKey epochKey = LogReplicationMetadataKey.newBuilder().setKey(maxEpochTableName).build();
        LogReplicationMetadataVal epochVal = LogReplicationMetadataVal.newBuilder().setVal(epoch).build();
        try {
            TxBuilder txBuilder = corfuStore.tx(namespace);
            if (epoch == getMaxEpoch()) {
                txBuilder.update(maxEpochTableName,
                        epochKey,
                        epochVal,
                        null);

                for (int i = 0; i < keys.size(); i++) {
                    LogReplicationMetadataKey tableKey = LogReplicationMetadataKey.newBuilder().setKey(keys.get(i)).build();
                    LogReplicationMetadataVal tableVal = LogReplicationMetadataVal.newBuilder().setVal(vals.get(i)).build();
                    txBuilder.update(getTableName(epoch),
                            tableKey,
                            tableVal,
                            null);
                }
            }
            txBuilder.commit();
        } catch (Exception e) {
            log.error("Caught an exception ", e);
            throw e;
        }
    }


    public enum PersistedWriterMetadataType {
        SnapshotEpic("snapshotEpic"),
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

    /**
     * check epoch is the max epoch and ts is larger than the previous baseSnapshot value
     * update epoch and baseSnapshot value
     * @param epoch
     * @param ts
     * @return
     */
    public long setSrcBaseSnapshotStart(long epoch, long ts) {
        List<String> keys = new ArrayList<>();
        List<Long> vals = new ArrayList<>();
        keys.add(PersistedWriterMetadata.PersistedWriterMetadataType.LastSnapStart.getVal());
        vals.add(ts);
        updateEpochAndKey(epoch, keys, vals);


        /*TxBuilder txBuilder = corfuStore.tx(namespace);
        if (epoch == getMaxEpoch()) {
            txBuilder.update(maxEpochTableName,
                    epochKey,
                    epochVal,
                    null);

            for (int i = 0; i < keys.size(); i++) {
                LogReplicationMetadataKey tableKey = LogReplicationMetadataKey.newBuilder().setKey(keys.get(i)).build();
                LogReplicationMetadataVal tableVal = LogReplicationMetadataVal.newBuilder().setVal(vals.get(i)).build();
                txBuilder.update(getTableName(epoch),
                        tableKey,
                        tableVal,
                        null);
            }
        }
        txBuilder.commit();
        /*
        long persistedEpic = 0;
        long persistedTs = 0;
        int retry = 0;
        boolean doRetry = true;
        while (retry++ < NUM_RETRY_WRITE && doRetry) {
            try {
                runtime.getObjectsView().TXBegin();
                persistedEpic = writerMetaDataTable.get(PersistedWriterMetadataType.SnapshotEpic.getVal());
                persistedTs = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());

                if (ts >= persistedTs) {
                    writerMetaDataTable.put(PersistedWriterMetadataType.SnapshotEpic.getVal(), ++persistedEpic);
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
                persistedTs = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());
            }
        }

        snapshotEpic = writerMetaDataTable.get(PersistedWriterMetadataType.SnapshotEpic.getVal());
        lastBaseSnapshotStart = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());
        return lastBaseSnapshotStart;
        */
        return 0;
    }
}
