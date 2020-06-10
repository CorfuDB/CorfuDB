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
public class LogReplicationMetadata {
    private static final String namespace = "CORFU_SYSTEM";
    private static final String TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";
    String metadataTableName;

    private CorfuStore corfuStore;

    Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> metadataTable;

    CorfuRuntime runtime;

    public LogReplicationMetadata(CorfuRuntime rt, long siteConfigID, UUID primary, UUID dst) {
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

    String queryString(CorfuStoreMetadata.Timestamp timestamp, PersistedWriterMetadataType key) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        CorfuRecord record;
        if (timestamp == null) {
            record = corfuStore.query(namespace).getRecord(metadataTableName, txKey);
        } else {
            record = corfuStore.query(namespace).getRecord(metadataTableName, timestamp, txKey);
        }

        LogReplicationMetadataVal metadataVal = null;
        String val = null;

        if (record != null) {
            metadataVal = (LogReplicationMetadataVal)record.getPayload();
        }

        if (metadataVal != null) {
            val = metadataVal.getVal();
        }

        //System.out.print("\nquery timestamp " + timestamp + " record " + record + " metadataVal: " + metadataVal + " val " + val);
        return val;
    }

    long query(CorfuStoreMetadata.Timestamp timestamp, PersistedWriterMetadataType key) {
        long val = -1;
        String str = queryString(timestamp, key);
        if (str != null) {
            val = Long.parseLong(str);
        }
        return val;
    }

    public long getSiteConfigID() {
        return query(null, PersistedWriterMetadataType.SiteConfigID);
    }

    public String getVersion() { return queryString(null, PersistedWriterMetadataType.Version); }

    public long getLastSnapStartTimestamp() {
        return query(null, PersistedWriterMetadataType.LastSnapshotStarted);
    }


    public long getLastSnapTransferDoneTimestamp() {
        return query(null, PersistedWriterMetadataType.LastSnapshotTransferred);
    }

    public long getLastSrcBaseSnapshotTimestamp() {
        return query(null, PersistedWriterMetadataType.LastSnapshotApplied);
    }

    public long getLastSnapSeqNum() {
        return query(null, PersistedWriterMetadataType.LastSnapshotSeqNum);
    }

    public long getLastProcessedLogTimestamp() {
        return query(null, PersistedWriterMetadataType.LastLogProcessed);
    }

    void appendUpdate(TxBuilder txBuilder, PersistedWriterMetadataType key, long val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(Long.toString(val)).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    void appendUpdate(TxBuilder txBuilder, PersistedWriterMetadataType key, String val) {
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
     * @return if the operation succeeds or not.
     */
    public boolean setSrcBaseSnapshotStart(long siteConfigID, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistSiteConfigID = query(timestamp, PersistedWriterMetadataType.SiteConfigID);
        long persistSnapStart = query(timestamp, PersistedWriterMetadataType.LastSnapshotStarted);

        log.debug("Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        // It means the site config has changed, ingore the update operation.
        if (siteConfigID != persistSiteConfigID || ts <= persistSiteConfigID) {
            log.warn("The metadata is older than the presisted one. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                    " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);
            return false;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        //Update the siteConfigID to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, PersistedWriterMetadataType.SiteConfigID, siteConfigID);

        //Setup the LastSnapshotStarted
        appendUpdate(txBuilder, PersistedWriterMetadataType.LastSnapshotStarted, ts);

        //Setup LastSnapshotSeqNum
        appendUpdate(txBuilder, PersistedWriterMetadataType.LastSnapshotSeqNum, Address.NON_ADDRESS);
        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        return (ts == getLastSnapStartTimestamp() && siteConfigID == getSiteConfigID());
    }


    /**
     * This call should be done in a transaction after a transfer done and before apply the snapshot.
     * @param ts
     */
    public void setLastSnapTransferDoneTimestamp(long siteConfigID, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persisteSiteConfigID = query(timestamp, PersistedWriterMetadataType.SiteConfigID);
        long persistSnapStart = query(timestamp, PersistedWriterMetadataType.LastSnapshotStarted);

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

        //Setup the LastSnapshotStarted
        appendUpdate(txBuilder, PersistedWriterMetadataType.LastSnapshotTransferred, ts);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persisteSiteConfigID " + persisteSiteConfigID + " persistSnapStart " + persistSnapStart);
        return;
    }

    public void setSnapshotApplied(LogReplicationEntry entry) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistSiteConfigID = query(timestamp, PersistedWriterMetadataType.SiteConfigID);
        long persistSnapStart = query(timestamp, PersistedWriterMetadataType.LastSnapshotStarted);
        long persistSnapTranferDone = query(timestamp, PersistedWriterMetadataType.LastSnapshotTransferred);
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

        appendUpdate(txBuilder, PersistedWriterMetadataType.LastSnapshotApplied, ts);
        appendUpdate(txBuilder, PersistedWriterMetadataType.LastLogProcessed, ts);

        //may not need
        appendUpdate(txBuilder, PersistedWriterMetadataType.LastSnapshotSeqNum, Address.NON_ADDRESS);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        return;
    }

    public String getMetadata() {
        String s = new String();
        s.concat(PersistedWriterMetadataType.SiteConfigID.getVal() + " " + getSiteConfigID() +" ");
        s.concat(PersistedWriterMetadataType.LastSnapshotStarted.getVal() + " " + getLastSnapStartTimestamp() +" ");
        s.concat(PersistedWriterMetadataType.LastSnapshotTransferred.getVal() + " " + getLastSnapTransferDoneTimestamp() + " ");
        s.concat(PersistedWriterMetadataType.LastSnapshotApplied.getVal() + " " + getLastSrcBaseSnapshotTimestamp() + " ");
        s.concat(PersistedWriterMetadataType.LastSnapshotSeqNum.getVal() + " " + getLastSnapSeqNum() + " ");
        s.concat(PersistedWriterMetadataType.LastLogProcessed.getVal() + " " + getLastProcessedLogTimestamp() + " ");

        return s;
    }

    public static String getPersistedWriterMetadataTableName(UUID primarySite, UUID dst) {
        return TABLE_PREFIX_NAME + primarySite.toString() + "-to-" + dst.toString();
    }

    public enum PersistedWriterMetadataType {
        SiteConfigID("SiteConfigID"),
        Version("Version"),
        LastSnapshotStarted("lastSnapStart"),
        LastSnapshotTransferred("lastSnapTransferred"),
        LastSnapshotApplied("lastSnapApplied"),
        LastSnapshotSeqNum("lastSnapSeqNum"),
        LastLogProcessed("lastLogProcessed");

        @Getter
        String val;
        PersistedWriterMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
