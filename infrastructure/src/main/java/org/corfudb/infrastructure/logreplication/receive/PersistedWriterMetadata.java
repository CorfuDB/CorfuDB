package org.corfudb.infrastructure.logreplication.receive;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationWriterMetadata.LogReplicationMetadataKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationWriterMetadata.LogReplicationMetadataVal;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.Address;
import java.util.UUID;

/**
 * The table persisted at the replication writer side.
 * It records the logreader cluster's snapshot timestamp  and last log entry's timestamp, it has received and processed.
 */
@Slf4j
public class PersistedWriterMetadata {
    private static final String namespace = "CORFU_SYSTEM";
    private static final String TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";
    String metadataTableName;

    private CorfuStore corfuStore;

    Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> metadataTable;

    CorfuRuntime runtime;

    public PersistedWriterMetadata(CorfuRuntime rt, long siteConfigID, UUID primary, UUID dst) {
        this.runtime = rt;
        this.corfuStore = new CorfuStore(runtime);
        metadataTableName = getPersistedWriterMetadataTableName(primary, dst);
        try {
            metadataTable = this.corfuStore.openTable(namespace,
                    metadataTableName,
                    LogReplicationMetadataKey.class,
                    LogReplicationMetadataVal.class,
                    null,
                    TableOptions.builder().build());
        } catch (Exception e) {
            log.error("Caught an exception while open the table");
            throw new ReplicationWriterException(e);
        }
        setupSiteConfigID(siteConfigID);
    }

    CorfuStoreMetadata.Timestamp getTimestamp() {
        return corfuStore.getTimestamp();
    }

    TxBuilder getTxBuilder() {
        return corfuStore.tx(namespace);
    }

    long query(CorfuStoreMetadata.Timestamp timestamp, PersistedWriterMetadataType key) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        CorfuRecord record;
        if (timestamp == null) {
            record = corfuStore.query(namespace).getRecord(metadataTableName, txKey);
        } else {
            record = corfuStore.query(namespace).getRecord(metadataTableName, timestamp, txKey);
        }

        LogReplicationMetadataVal metadataVal = null;
        long val = -1;

        if (record != null) {
            metadataVal = (LogReplicationMetadataVal)record.getPayload();
        }

        if (metadataVal != null) {
            val = metadataVal.getVal();
        }

        //System.out.print("\nquery timestamp " + timestamp + " record " + record + " metadataVal: " + metadataVal + " val " + val);
        return val;
    }

    public long getSiteConfigID() {
        return query(null, PersistedWriterMetadataType.SiteConfigID);
    }

    public long getLastSnapStartTimestamp() {
        return query(null, PersistedWriterMetadataType.LastSnapStart);
    }


    public long getLastSnapTransferDoneTimestamp() {
        return query(null, PersistedWriterMetadataType.LastSnapTransferDone);
    }

    public long getLastSrcBaseSnapshotTimestamp() {
        return query(null, PersistedWriterMetadataType.LastSnapApplyDone);
    }

    public long getLastSnapSeqNum() {
        return query(null, PersistedWriterMetadataType.LastSnapSeqNum);
    }

    public long getLastProcessedLogTimestamp() {
        return query(null, PersistedWriterMetadataType.LastLogProcessed);
    }

    void appendUpdate(TxBuilder txBuilder, PersistedWriterMetadataType key, long val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(val).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }


    public void setupSiteConfigID(long siteConfigID) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistSiteConfigID = query(timestamp, PersistedWriterMetadataType.SiteConfigID);

        if (siteConfigID <= persistSiteConfigID) {
            log.warn("Skip setupSiteConfigID. the current siteConfigID " + siteConfigID + " is not larger than the persistSiteConfigID " + persistSiteConfigID);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        for (PersistedWriterMetadataType key : PersistedWriterMetadataType.values()) {
            long val = Address.NON_ADDRESS;
            if (key == PersistedWriterMetadataType.SiteConfigID) {
                val = siteConfigID;
            }
            appendUpdate(txBuilder, key, val);
         }

        txBuilder.commit(timestamp);
    }

    /**
     * If the current siteConfigID is not the same as the persisted siteConfigID, ignore the operation.
     * If the current ts is smaller than the persisted snapStart, it is an old operation,
     * ignore it it.
     * Otherwise, update the snapStart. The update of siteConfigID just fence off any other metadata
     * updates in another transactions.
     *
     * @param siteConfigID the current operation's siteConfigID
     * @param ts the snapshotStart snapshot time for the siteConfigID.
     * @return the persistent snapshotStart
     */
    public void setSrcBaseSnapshotStart(long siteConfigID, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistSiteConfigID = query(timestamp, PersistedWriterMetadataType.SiteConfigID);
        long persistSnapStart = query(timestamp, PersistedWriterMetadataType.LastSnapStart);

        log.debug("Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        // It means the site config has changed, ingore the update operation.
        if (siteConfigID != persistSiteConfigID || ts <= persistSiteConfigID) {
            log.warn("The metadata is older than the presisted one. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                    " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        //Update the siteConfigID to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, PersistedWriterMetadataType.SiteConfigID, siteConfigID);

        //Setup the LastSnapStart
        appendUpdate(txBuilder, PersistedWriterMetadataType.LastSnapStart, ts);

        //Setup LastSnapSeqNum
        appendUpdate(txBuilder, PersistedWriterMetadataType.LastSnapSeqNum, Address.NON_ADDRESS);
        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        return;
    }


    /**
     * This call should be done in a transaction after a transfer done and before apply the snapshot.
     * @param ts
     */
    public void setLastSnapTransferDoneTimestamp(long siteConfigID, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persisteSiteConfigID = query(timestamp, PersistedWriterMetadataType.SiteConfigID);
        long persistSnapStart = query(timestamp, PersistedWriterMetadataType.LastSnapStart);

        log.debug("setLastSnapTransferDone snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persisteSiteConfigID " + persisteSiteConfigID + " persistSnapStart " + persistSnapStart);

        // It means the site config has changed, ingore the update operation.
        if (siteConfigID != persisteSiteConfigID || ts <= persisteSiteConfigID) {
            log.warn("The metadata is older than the presisted one. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                    " persisteSiteConfigID " + persisteSiteConfigID + " persistSnapStart " + persistSnapStart);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        //Update the siteConfigID to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, PersistedWriterMetadataType.SiteConfigID, siteConfigID);

        //Setup the LastSnapStart
        appendUpdate(txBuilder, PersistedWriterMetadataType.LastSnapTransferDone, ts);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persisteSiteConfigID " + persisteSiteConfigID + " persistSnapStart " + persistSnapStart);
        return;
    }

    public void setSrcBaseSnapshotDone(LogReplicationEntry entry) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistSiteConfigID = query(timestamp, PersistedWriterMetadataType.SiteConfigID);
        long persistSnapStart = query(timestamp, PersistedWriterMetadataType.LastSnapStart);
        long persistSnapTranferDone = query(timestamp, PersistedWriterMetadataType.LastSnapTransferDone);
        long siteConfigID = entry.getMetadata().getSiteConfigID();
        long ts = entry.getMetadata().getSnapshotTimestamp();

        if (siteConfigID != persistSiteConfigID || ts != persistSnapStart || ts != persistSnapTranferDone) {
            log.warn("siteConfigID " + siteConfigID + " != " + " persist " + persistSiteConfigID +  " ts " + ts +
                    " != " + "persistSnapTranferDone " + persistSnapTranferDone);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        //Update the siteConfigID to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, PersistedWriterMetadataType.SiteConfigID, siteConfigID);

        appendUpdate(txBuilder, PersistedWriterMetadataType.LastSnapApplyDone, ts);
        appendUpdate(txBuilder, PersistedWriterMetadataType.LastLogProcessed, ts);

        //may not need
        appendUpdate(txBuilder, PersistedWriterMetadataType.LastSnapSeqNum, Address.NON_ADDRESS);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        return;
    }

    public static String getPersistedWriterMetadataTableName(UUID primarySite, UUID dst) {
        return TABLE_PREFIX_NAME + primarySite.toString() + "-to-" + dst.toString();
    }

    public enum PersistedWriterMetadataType {
        SiteConfigID("SiteConfigID"),
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
