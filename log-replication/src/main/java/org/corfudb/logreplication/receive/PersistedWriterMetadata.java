package org.corfudb.logreplication.receive;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.Serializers;

import java.util.Map;
import java.util.UUID;

/**
 * The table persisted at the replication writer side.
 * It records the reader cluster's snapshot timestamp  and last log entry's timestamp, it has received and processed.
 */
@Slf4j
public class PersistedWriterMetadata {
    private static final String TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";
    private static final int NUM_RETRY_WRITE = 3;

    // this is for internal use to check if the epic has been changed by another
    // node or process.
    private long snapshotEpic;

    // this is for internal use to check if the lastBaseSnapshotStart has been changed by
    // another node or process.
    private long lastBaseSnapshotStart;

    private Map<String, Long> writerMetaDataTable;

    CorfuRuntime runtime;

    public PersistedWriterMetadata(CorfuRuntime rt, UUID dst) {
        this.runtime = rt;
        writerMetaDataTable = rt.getObjectsView()
                .build()
                .setStreamName(getPersistedWriterMetadataTableName(dst))
                .setTypeToken(new TypeToken<CorfuTable<String, Long>>() {
                })
                .setSerializer(Serializers.PRIMITIVE)
                .open();

        try {
            rt.getObjectsView().TXBegin();
            if (writerMetaDataTable.isEmpty()) {
                writerMetaDataTable.put(PersistedWriterMetadataType.SnapshotEpic.getVal(), Address.NON_ADDRESS);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapStart.getVal(), Address.NON_ADDRESS);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapTransferDone.getVal(), Address.NON_ADDRESS);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapApplyDone.getVal(), Address.NON_ADDRESS);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastLogProcessed.getVal(), Address.NON_ADDRESS);
            }

        } catch (TransactionAbortedException e) {
            log.debug("Caught an exception {}", e.getStackTrace());
            log.warn("Transaction is aborted with writerMetadataTable.size {} ", writerMetaDataTable.size());
        } finally {
            rt.getObjectsView().TXEnd();
            snapshotEpic = writerMetaDataTable.get(PersistedWriterMetadataType.SnapshotEpic.getVal());
            lastBaseSnapshotStart = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());
        }
    }

    public static String getPersistedWriterMetadataTableName(UUID dst) {
        return TABLE_PREFIX_NAME + dst.toString();
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
    public long setSrcBaseSnapshotStart(long ts) {
        long persistedEpic = 0;
        long persistedTs = 0;
        int retry = 0;
        boolean doRetry = true;
        while (retry++ < NUM_RETRY_WRITE && doRetry) {
            retry++;
            try {
                runtime.getObjectsView().TXBegin();
                persistedEpic = writerMetaDataTable.get(PersistedWriterMetadataType.SnapshotEpic.getVal());
                persistedTs = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());

                if (ts >= persistedTs) {
                    writerMetaDataTable.put(PersistedWriterMetadataType.SnapshotEpic.getVal(), ++persistedEpic);
                    writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapStart.getVal(), ts);
                    //TODO:  clean persistentQue if no AR
                }

                doRetry = false;
            } catch (TransactionAbortedException e) {
                log.debug("Caught transaction aborted exception {}", e.getStackTrace());
                //todo maxi, should we throw a new type of exception to stop snapshot sync.

                log.warn("While trying to update lastSnapStart value to {}, aborted with retry {}", ts, retry);
                System.out.println("While trying to update lastSnapStart value " + ts +" aborted with retry " + retry);
            } finally {
                runtime.getObjectsView().TXEnd();
                persistedTs = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());
                if (ts != persistedTs) {
                    log.warn("The current LastSnapStart ts {} is not equal the persisted ts {} with retry {}. ",
                            ts, persistedTs, retry);
                }
                System.out.println("setSrcBaseSnapshotStart  " + ts + " persitedTs " + persistedTs);
            }
        }

        snapshotEpic = writerMetaDataTable.get(PersistedWriterMetadataType.SnapshotEpic.getVal());
        lastBaseSnapshotStart = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());
        return lastBaseSnapshotStart;
    }


    /**
     * If the persistent data show it is my epic and my snapshot value, will update the
     * snapshot timestamp and the lastlogprocessed timestamp
     */
    public void setSrcBaseSnapshotDone() {
        try {
            runtime.getObjectsView().TXBegin();
            long epic = writerMetaDataTable.get(PersistedWriterMetadataType.SnapshotEpic.getVal());
            long ts = writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapStart.getVal());
            if (epic == snapshotEpic && lastBaseSnapshotStart == ts) {
                writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapTransferDone.getVal(), ts);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastSnapApplyDone.getVal(), ts);
                writerMetaDataTable.put(PersistedWriterMetadataType.LastLogProcessed.getVal(), ts);
            }
        } catch (TransactionAbortedException e) {
            log.warn("Transaction is aborted. The snapshot has been updated by someone else");
        } finally {
            runtime.getObjectsView().TXEnd();
        }
    }

    /**
     * This call should be done in a transaction while applying a log entry message.
     * Only update the lastLogProcesed timestamp
     * @param ts
     */
    public void setLastProcessedLogTimestamp(long ts) {
        writerMetaDataTable.put(PersistedWriterMetadataType.LastLogProcessed.getVal(), ts);
    }

    public long getLastSrcBaseSnapshotTimestamp() {
        return writerMetaDataTable.get(PersistedWriterMetadataType.LastSnapApplyDone.getVal());
    }

    public long getLastProcessedLogTimestamp() {
        return writerMetaDataTable.get(PersistedWriterMetadataType.LastLogProcessed.getVal());
    }

    public enum PersistedWriterMetadataType {
        SnapshotEpic("snapshotEpic"),
        LastSnapStart("lastSnapStart"),
        LastSnapTransferDone("lastSnapTransferDone"),
        LastSnapApplyDone("lastSnapApplied"),
        LastLogProcessed("lastLogProcessed");

        @Getter
        String val;
        PersistedWriterMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
