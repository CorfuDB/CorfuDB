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
 * The log replication metadata is stored in a corfutable in the corfustore.
 * The log replication metadata is defined as a proto message that contains:
 * SiteConfigID, Version, Snapshot Full Sync Status and Log Entry Sync Status.
 * The access the metadata is using UFO API.
 *
 * To record replication status, it has following values:
 * SnapshotStartTimestamp: when a full snapshot sync is started, it will first update this value and reset other snapshot related metadata to -1.
 * The init value for this metadata is -1. When it is -1, it means a snapshot full sync is required regardless.
 * SnapshotTranferredTimestamp: the init value is -1. When the receiver receives a snapshot transfer end marker, it will update this value to current snapshot timestamp,
 * it will be updated to the same value as snapshot start when the snapshot data transfer is done.
 * SnapshotSeqNum: it the sequence number of each snapshot messages to detect loss of messages and also to prevent the reappling the same message. All the messages must be
 * applied in the order of the snapshot sequence number.
 * SnapshotAppliedSeqNum: it records the operation's sequence during the apply phase to avoid the redo the apply if there is a leadership change.
 * LastLogProcessed: It records the most recent log entry has been processed.
 * When a snapshot full sync is complete, it will update this value. While processing a new log entry message, it will be updated too.
 *
 * While update siteConfigID or version number, the replication status will all be reset to -1 to
 * require a snapshot full sync.
 */
@Slf4j
public class LogReplicationMetadataAccessor {
    private static final String namespace = "CORFU_SYSTEM";
    private static final String TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";
    private static final String DEFAULT_VERSION = "Release_Test_0";
    String metadataTableName;

    private CorfuStore corfuStore;

    /**
     * Table used to store the log replication status
     */
    Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> metadataTable;

    CorfuRuntime runtime;

    public LogReplicationMetadataAccessor(CorfuRuntime rt, long siteConfigID, UUID primary, UUID dst) {
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

    /**
     * Get the corfustore log tail.
     * @return
     */
    CorfuStoreMetadata.Timestamp getTimestamp() {
        return corfuStore.getTimestamp();
    }

    /**
     * create a tx builder
     * @return
     */
    TxBuilder getTxBuilder() {
        return corfuStore.tx(namespace);
    }

    /**
     * Given a specific metadata key type, get the corresponding value in string format.
     * @param timestamp
     * @param key
     * @return
     */
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

    /**
     * Given a specific metadata key type and timestamp, get the corresponding long value.
     * @param timestamp
     * @param key
     * @return
     */

    long query(CorfuStoreMetadata.Timestamp timestamp, LogReplicationMetadataType key) {
        long val = -1;
        String str = queryString(timestamp, key);
        if (str != null) {
            val = Long.parseLong(str);
        }
        return val;
    }

    /**
     * Given a specific timestamp, get the current siteConfig metadata value.
     * If the timestamp is null, get the most recent value.
     * @param ts
     * @return
     */
    public long getSiteConfigID(CorfuStoreMetadata.Timestamp ts) {
        return query(ts, LogReplicationMetadataType.SiteConfigID);
    }

    /**
     * Given a specific timestamp, get the current version value.
     * If the timestamp is null, get the most recent value.
     * @param ts
     * @return
     */
    public String getVersion(CorfuStoreMetadata.Timestamp ts) { return queryString(ts, LogReplicationMetadataType.Version); }


    /**
     * Given a specific timestamp, get the most recent started full snapshot sync's timestamp.
     * If the timestamp is null, get the most recent value.
     * @param ts
     * @return
     */
    public long getLastSnapStartTimestamp(CorfuStoreMetadata.Timestamp ts) {
        return query(ts, LogReplicationMetadataType.LastSnapshotStarted);
    }

    /**
     * Given a specific timestamp, get the most recent full snapshot sync's timestamp that has complete the
     * transferred phase.
     * If the timestamp is null, get the most recent value.
     * @param ts
     * @return
     */
    public long getLastSnapTransferDoneTimestamp(CorfuStoreMetadata.Timestamp ts) {
        return query(ts, LogReplicationMetadataType.LastSnapshotTransferred);
    }

    /**
     * Given a specific timestamp, get the most recent completed snapshot full sync's timestamp.
     * If the timestamp is null, get the most recent value.
     * @param ts
     * @return
     */
    public long getLastSrcBaseSnapshotTimestamp(CorfuStoreMetadata.Timestamp ts) {
        return query(ts, LogReplicationMetadataType.LastSnapshotApplied);
    }

    /**
     * Given a specific timestamp, get the most recent snapshot message's sequence number
     * that the receiver has applied to the shadow streams.
     * If the timestamp is null, get the most recent value.
     * @param ts
     * @return
     */
    public long getLastSnapSeqNum(CorfuStoreMetadata.Timestamp ts) {
        return query(ts, LogReplicationMetadataType.LastSnapshotSeqNum);
    }

    /**
     * Given a specific timestamp, get the most recent applied operation sequence number
     * that the receiver has applied to the real streams.
     * If the timestamp is null, get the most recent value.
     * @param ts
     * @return
     */
    public long getLastSnapAppliedSeqNum(CorfuStoreMetadata.Timestamp ts) {
        return query(ts, LogReplicationMetadataType.LastSnapshotAppliedSeqNum);
    }

    /**
     * Given a specific timestamp, get the most recent log that has applied to the
     * receiver's log.
     * If the timestamp is null, get the most recent value.
     * @param ts
     * @return
     */
    public long getLastProcessedLogTimestamp(CorfuStoreMetadata.Timestamp ts) {
        return query(ts, LogReplicationMetadataType.LastLogProcessed);
    }

    /**
     * Append an metadata update to the same tx with a long.
     * @param txBuilder
     * @param key the key to be updated.
     * @param val the new value.
     */
    void appendUpdate(TxBuilder txBuilder, LogReplicationMetadataType key, long val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(Long.toString(val)).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    /**
     * Append an metadata update to the same tx with a String.
     * @param txBuilder
     * @param key the key to be updated.
     * @param val the new value.
     */
    void appendUpdate(TxBuilder txBuilder, LogReplicationMetadataType key, String val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(val).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    /**
     * Update the siteConfigID if it is bigger than the current one.
     * At the same time reset replication status metadata to -1.
     * @param siteConfigID
     */
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
            } else if (key == LogReplicationMetadataType.Version) {
                String version;
                version = getVersion(null);
                if (version == null) {
                    appendUpdate(txBuilder, key, DEFAULT_VERSION);
                }
                continue;
            }
            appendUpdate(txBuilder, key, val);
         }

        if (getVersion(null) == null) {

        }
        txBuilder.commit(timestamp);
        log.info("Update siteConfigID {}, new metadata {}", siteConfigID, getLogReplicationStatus());
    }


    /**
     * Update the version value and reset log replication related metadata except siteConfigID.
     * @param version
     */
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
        appendUpdate(txBuilder, LogReplicationMetadataType.LastSnapshotAppliedSeqNum, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LastLogProcessed, Address.NON_ADDRESS);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        return (ts == getLastSnapStartTimestamp(null) && siteConfigID == getSiteConfigID(null));
    }


    /**
     * While snapshot full sync has finished the phase I: transferred all data from sender to receiver and applied to
     * the shadow streams.
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

    /**
     * When the snapshot full sync completes both phase I, data transfer, and phase II, applying to the real data
     * streams at the receiver, update the full snapshot complete value.
     * @param entry
     */
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

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart siteConfigID " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        return;
    }

    /**
     * Get all metadata in a user friendly string format
     * @return
     */
    public String getLogReplicationStatus() {
        String s = new String();
        CorfuStoreMetadata.Timestamp ts = getTimestamp();
        s = s.concat(LogReplicationMetadataType.SiteConfigID.getVal() + " " + getSiteConfigID(ts) +" ");
        s = s.concat(LogReplicationMetadataType.LastSnapshotStarted.getVal() + " " + getLastSnapStartTimestamp(ts) +" ");
        s = s.concat(LogReplicationMetadataType.LastSnapshotTransferred.getVal() + " " + getLastSnapTransferDoneTimestamp(ts) + " ");
        s = s.concat(LogReplicationMetadataType.LastSnapshotApplied.getVal() + " " + getLastSrcBaseSnapshotTimestamp(ts) + " ");
        s = s.concat(LogReplicationMetadataType.LastSnapshotSeqNum.getVal() + " " + getLastSnapSeqNum(ts) + " ");
        s = s.concat(LogReplicationMetadataType.LastSnapshotAppliedSeqNum.getVal() + " " + getLastSnapAppliedSeqNum(ts) + " ");
        s = s.concat(LogReplicationMetadataType.LastLogProcessed.getVal() + " " + getLastProcessedLogTimestamp(ts) + " ");
        log.info("metadata {}", s);
        return s;
    }

    /**
     * The metadata table name
     * @param primarySite
     * @param dst
     * @return
     */
    public static String getPersistedWriterMetadataTableName(UUID primarySite, UUID dst) {
        return TABLE_PREFIX_NAME + primarySite.toString() + "-to-" + dst.toString();
    }

    /**
     * All types of metadata stored in the corfu table, those corresponding strings are used as keys to access
     * the real values.
     */
    public enum LogReplicationMetadataType {
        SiteConfigID("SiteConfigID"),
        Version("Version"),
        LastSnapshotStarted("lastSnapStart"),
        LastSnapshotTransferred("lastSnapTransferred"),
        LastSnapshotApplied("lastSnapApplied"),
        LastSnapshotSeqNum("lastSnapSeqNum"),
        LastSnapshotAppliedSeqNum("lastSnapAppliedSeqNum"),
        LastLogProcessed("lastLogProcessed");

        @Getter
        String val;
        LogReplicationMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
