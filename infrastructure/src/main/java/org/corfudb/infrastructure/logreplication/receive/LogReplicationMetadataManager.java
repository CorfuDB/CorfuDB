package org.corfudb.infrastructure.logreplication.receive;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataVal;
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
public class LogReplicationMetadataManager {
    private static final String namespace = "CORFU_SYSTEM";
    private static final String TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";
    String metadataTableName;

    private CorfuStore corfuStore;

    Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> metadataTable;

    CorfuRuntime runtime;

    public LogReplicationMetadataManager(CorfuRuntime rt, long siteConfigID, UUID primary, UUID dst) {
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

    String queryString(CorfuStoreMetadata.Timestamp timestamp, LogReplicationMetadataType key) {
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

        return val;
    }

    long query(CorfuStoreMetadata.Timestamp timestamp, LogReplicationMetadataType key) {
        long val = -1;
        String str = queryString(timestamp, key);
        if (str != null) {
            val = Long.parseLong(str);
        }
        return val;
    }

    public long getSiteConfigID() {
        return query(null, LogReplicationMetadataType.SiteConfigID);
    }

    public String getVersion() { return queryString(null, LogReplicationMetadataType.Version); }

    public long getLastSnapStartTimestamp() {
        return query(null, LogReplicationMetadataType.LastSnapshotStarted);
    }


    public long getLastSnapTransferDoneTimestamp() {
        return query(null, LogReplicationMetadataType.LastSnapshotTransferred);
    }

    public long getLastSrcBaseSnapshotTimestamp() {
        return query(null, LogReplicationMetadataType.LastSnapshotApplied);
    }

    public long getLastSnapSeqNum() {
        return query(null, LogReplicationMetadataType.LastSnapshotSeqNum);
    }

    public long getLastProcessedLogTimestamp() {
        return query(null, LogReplicationMetadataType.LastLogProcessed);
    }

    void appendUpdate(TxBuilder txBuilder, LogReplicationMetadataType key, long val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(Long.toString(val)).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    void appendUpdate(TxBuilder txBuilder, LogReplicationMetadataType key, String val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(val).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    public void setupSiteConfigID(long siteConfigID) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistSiteConfigID = query(timestamp, LogReplicationMetadataType.SiteConfigID);

        if (siteConfigID <= persistSiteConfigID) {
            log.warn("Skip setupSiteConfigID. the current siteConfigID " + siteConfigID + " is not larger than the persistSiteConfigID " + persistSiteConfigID);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        for (LogReplicationMetadataType key : LogReplicationMetadataType.values()) {
            long val = Address.NON_ADDRESS;
            if (key == LogReplicationMetadataType.SiteConfigID) {
                val = siteConfigID;
            }
            appendUpdate(txBuilder, key, val);
         }

        txBuilder.commit(timestamp);
        log.info("Update siteConfigID, new metadata {}", getMetadata());
    }

    public void updateVersion(String version) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        String  persistVersion = queryString(timestamp, LogReplicationMetadataType.Version);

        if (persistVersion.equals(version)) {
            log.warn("Skip update the current version {} with new version {} as they are the same", persistVersion, version);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        for (LogReplicationMetadataType key : LogReplicationMetadataType.values()) {
            long val = Address.NON_ADDRESS;

            // For version, it will be updated with the current version
            if (key == LogReplicationMetadataType.Version) {
                appendUpdate(txBuilder, key, version);
            } else if (key == LogReplicationMetadataType.SiteConfigID) {
                // For siteConfig ID, it should not be changed. Update it to fence off other metadata updates.
                val = query(timestamp, LogReplicationMetadataType.SiteConfigID);
                appendUpdate(txBuilder, key, val);
            } else {
                // Reset all other keys to -1.
                appendUpdate(txBuilder, key, val);
            }
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
        long persistSiteConfigID = query(timestamp, LogReplicationMetadataType.SiteConfigID);
        long persistSnapStart = query(timestamp, LogReplicationMetadataType.LastSnapshotStarted);

        log.debug("Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        // It means the site config has changed, ingore the update operation.
        if (siteConfigID != persistSiteConfigID || ts <= persistSiteConfigID) {
            log.warn("The metadata is older than the presisted one. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                    " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);
            return false;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        // Update the siteConfigID to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.SiteConfigID, siteConfigID);

        // Setup the LastSnapshotStarted
        appendUpdate(txBuilder, LogReplicationMetadataType.LastSnapshotStarted, ts);

        // Reset other metadata
        appendUpdate(txBuilder, LogReplicationMetadataType.LastSnapshotTransferred, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LastSnapshotApplied, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LastSnapshotSeqNum, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LastLogProcessed, Address.NON_ADDRESS);

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
        long persisteSiteConfigID = query(timestamp, LogReplicationMetadataType.SiteConfigID);
        long persistSnapStart = query(timestamp, LogReplicationMetadataType.LastSnapshotStarted);

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
        appendUpdate(txBuilder, LogReplicationMetadataType.SiteConfigID, siteConfigID);

        //Setup the LastSnapshotStarted
        appendUpdate(txBuilder, LogReplicationMetadataType.LastSnapshotTransferred, ts);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persisteSiteConfigID " + persisteSiteConfigID + " persistSnapStart " + persistSnapStart);
        return;
    }

    public void setSnapshotApplied(LogReplicationEntry entry) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistSiteConfigID = query(timestamp, LogReplicationMetadataType.SiteConfigID);
        long persistSnapStart = query(timestamp, LogReplicationMetadataType.LastSnapshotStarted);
        long persistSnapTranferDone = query(timestamp, LogReplicationMetadataType.LastSnapshotTransferred);
        long siteConfigID = entry.getMetadata().getSiteConfigID();
        long ts = entry.getMetadata().getSnapshotTimestamp();

        if (siteConfigID != persistSiteConfigID || ts != persistSnapStart || ts != persistSnapTranferDone) {
            log.warn("siteConfigID " + siteConfigID + " != " + " persist " + persistSiteConfigID +  " ts " + ts +
                    " != " + "persistSnapTranferDone " + persistSnapTranferDone);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        //Update the siteConfigID to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.SiteConfigID, siteConfigID);

        appendUpdate(txBuilder, LogReplicationMetadataType.LastSnapshotApplied, ts);
        appendUpdate(txBuilder, LogReplicationMetadataType.LastLogProcessed, ts);

        //may not need
        appendUpdate(txBuilder, LogReplicationMetadataType.LastSnapshotSeqNum, Address.NON_ADDRESS);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        return;
    }

    public String getMetadata() {
        String s = new String();
        s.concat(LogReplicationMetadataType.SiteConfigID.getVal() + " " + getSiteConfigID() +" ");
        s.concat(LogReplicationMetadataType.LastSnapshotStarted.getVal() + " " + getLastSnapStartTimestamp() +" ");
        s.concat(LogReplicationMetadataType.LastSnapshotTransferred.getVal() + " " + getLastSnapTransferDoneTimestamp() + " ");
        s.concat(LogReplicationMetadataType.LastSnapshotApplied.getVal() + " " + getLastSrcBaseSnapshotTimestamp() + " ");
        s.concat(LogReplicationMetadataType.LastSnapshotSeqNum.getVal() + " " + getLastSnapSeqNum() + " ");
        s.concat(LogReplicationMetadataType.LastLogProcessed.getVal() + " " + getLastProcessedLogTimestamp() + " ");

        return s;
    }

    public static String getPersistedWriterMetadataTableName(UUID primarySite, UUID dst) {
        return TABLE_PREFIX_NAME + primarySite.toString() + "-to-" + dst.toString();
    }

    public enum LogReplicationMetadataType {
        SiteConfigID("SiteConfigID"),
        Version("Version"),
        LastSnapshotStarted("lastSnapStart"),
        LastSnapshotTransferred("lastSnapTransferred"),
        LastSnapshotApplied("lastSnapApplied"),
        LastSnapshotSeqNum("lastSnapSeqNum"),
        LastLogProcessed("lastLogProcessed");

        @Getter
        String val;
        LogReplicationMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
