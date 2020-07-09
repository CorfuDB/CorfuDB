package org.corfudb.infrastructure.logreplication.replication.receive;

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

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * The table persisted at the replication writer side.
 * It records the log reader cluster's snapshot timestamp  and last log entry's timestamp, it has received and processed.
 */
@Slf4j
public class LogReplicationMetadataManager {

    private static final String namespace = CORFU_SYSTEM_NAMESPACE;
    private static final String TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";

    private CorfuStore corfuStore;

    private String metadataTableName;

    private Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> metadataTable;

    private CorfuRuntime runtime;

    public LogReplicationMetadataManager(CorfuRuntime rt, long topologyConfigId, String localClusterId) {
        this.runtime = rt;
        this.corfuStore = new CorfuStore(runtime);
        metadataTableName = getPersistedWriterMetadataTableName(localClusterId);
        try {
            metadataTable = this.corfuStore.openTable(namespace,
                            metadataTableName,
                            LogReplicationMetadataKey.class,
                            LogReplicationMetadataVal.class,
                            null,
                            TableOptions.builder().build());
        } catch (Exception e) {
            log.error("Caught an exception while opening the table namespace={}, name={}", namespace, metadataTableName);
            throw new ReplicationWriterException(e);
        }
        setupTopologyConfigId(topologyConfigId);
    }

    public CorfuStoreMetadata.Timestamp getTimestamp() {
        return corfuStore.getTimestamp();
    }

    public TxBuilder getTxBuilder() {
        return corfuStore.tx(namespace);
    }

    private String queryString(CorfuStoreMetadata.Timestamp timestamp, LogReplicationMetadataType key) {
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

    public long query(CorfuStoreMetadata.Timestamp timestamp, LogReplicationMetadataType key) {
        long val = -1;
        String str = queryString(timestamp, key);
        if (str != null) {
            val = Long.parseLong(str);
        }
        return val;
    }

    public long getTopologyConfigId() {
        return query(null, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
    }

    public String getVersion() { return queryString(null, LogReplicationMetadataType.VERSION); }

    public long getLastSnapStartTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
    }


    public long getLastSnapTransferDoneTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED);
    }

    public long getLastSrcBaseSnapshotTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED);
    }

    public long getLastSnapSeqNum() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM);
    }

    public long getLastProcessedLogTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_LOG_PROCESSED);
    }

    public void appendUpdate(TxBuilder txBuilder, LogReplicationMetadataType key, long val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(Long.toString(val)).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    private void appendUpdate(TxBuilder txBuilder, LogReplicationMetadataType key, String val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(val).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    public void setupTopologyConfigId(long topologyConfigId) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistedTopologyConfigId = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);

        if (topologyConfigId <= persistedTopologyConfigId) {
            log.warn("Skip setupTopologyConfigId. the current topologyConfigId " + topologyConfigId + " is not larger than the persistedTopologyConfigID " + persistedTopologyConfigId);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        for (LogReplicationMetadataType key : LogReplicationMetadataType.values()) {
            long val = Address.NON_ADDRESS;
            if (key == LogReplicationMetadataType.TOPOLOGY_CONFIG_ID) {
                val = topologyConfigId;
            }
            appendUpdate(txBuilder, key, val);
         }

        txBuilder.commit(timestamp);
        log.info("Update topologyConfigId, new metadata {}", this);
    }

    public void updateVersion(String version) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        String  persistedVersion = queryString(timestamp, LogReplicationMetadataType.VERSION);

        if (persistedVersion.equals(version)) {
            log.warn("Skip update the current version {} with new version {} as they are the same", persistedVersion, version);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        for (LogReplicationMetadataType key : LogReplicationMetadataType.values()) {
            long val = Address.NON_ADDRESS;

            // For version, it will be updated with the current version
            if (key == LogReplicationMetadataType.VERSION) {
                appendUpdate(txBuilder, key, version);
            } else if (key == LogReplicationMetadataType.TOPOLOGY_CONFIG_ID) {
                // For siteConfig ID, it should not be changed. Update it to fence off other metadata updates.
                val = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
                appendUpdate(txBuilder, key, val);
            } else {
                // Reset all other keys to -1.
                appendUpdate(txBuilder, key, val);
            }
        }

        txBuilder.commit(timestamp);
    }

    /**
     * If the current topologyConfigId is not the same as the persisted topologyConfigId, ignore the operation.
     * If the current ts is smaller than the persisted snapStart, it is an old operation,
     * ignore it it.
     * Otherwise, update the snapStart. The update of topologyConfigId just fence off any other metadata
     * updates in another transactions.
     *
     * @param topologyConfigId the current operation's topologyConfigId
     * @param ts the snapshotStart snapshot time for the topologyConfigId.
     * @return if the operation succeeds or not.
     */
    public boolean setSrcBaseSnapshotStart(long topologyConfigId, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistedTopologyConfigID = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistSnapStart = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);

        log.debug("Set snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}, persistedSnapshotStart={}",
                topologyConfigId, ts, persistedTopologyConfigID, persistSnapStart);

        // It means the cluster config has changed, ignore the update operation.
        if (topologyConfigId != persistedTopologyConfigID || ts <= persistedTopologyConfigID) {
            log.warn("The metadata is older than the persisted one. Set snapshotStart topologyConfigId={}, ts={}," +
                    " persistedTopologyConfigId={}, persistedSnapshotStart={}", topologyConfigId, ts,
                    persistedTopologyConfigID, persistSnapStart);
            return false;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        // Update the topologyConfigId to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);

        // Setup the LAST_SNAPSHOT_STARTED
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, ts);

        // Reset other metadata
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_LOG_PROCESSED, Address.NON_ADDRESS);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}, " +
                        "persistedSnapshotStart={}",
                topologyConfigId, ts, persistedTopologyConfigID, persistSnapStart);

        return (ts == getLastSnapStartTimestamp() && topologyConfigId == getTopologyConfigId());
    }


    /**
     * This call should be done in a transaction after a transfer done and before apply the snapshot.
     * @param ts
     */
    public void setLastSnapTransferDoneTimestamp(long topologyConfigId, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistedTopologyConfigId = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistSnapStart = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);

        log.debug("setLastSnapTransferDone snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}," +
                " persistedSiteConfigID={}, persistedSnapshotStart={}", topologyConfigId, ts, persistedTopologyConfigId,
                persistSnapStart);

        // It means the cluster config has changed, ignore the update operation.
        if (topologyConfigId != persistedTopologyConfigId || ts <= persistedTopologyConfigId) {
            log.warn("The metadata is older than the persisted one. Set snapshotStart topologyConfigId " + topologyConfigId + " ts " + ts +
                    " persisteSiteConfigID " + persistedTopologyConfigId + " persistSnapStart " + persistSnapStart);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        //Update the topologyConfigId to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);

        //Setup the LAST_SNAPSHOT_STARTED
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED, ts);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart topologyConfigId " + topologyConfigId + " ts " + ts +
                " persisteSiteConfigID " + persistedTopologyConfigId + " persistSnapStart " + persistSnapStart);
        return;
    }

    public void setSnapshotApplied(LogReplicationEntry entry) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistSiteConfigID = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistSnapStart = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistSnapTranferDone = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED);
        long siteConfigID = entry.getMetadata().getTopologyConfigId();
        long ts = entry.getMetadata().getSnapshotTimestamp();

        if (siteConfigID != persistSiteConfigID || ts != persistSnapStart || ts != persistSnapTranferDone) {
            log.warn("topologyConfigId " + siteConfigID + " != " + " persist " + persistSiteConfigID +  " ts " + ts +
                    " != " + "persistSnapTransferDone " + persistSnapTranferDone);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(namespace);

        //Update the topologyConfigId to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, siteConfigID);

        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, ts);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_LOG_PROCESSED, ts);

        //may not need
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM, Address.NON_ADDRESS);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart topologyConfigId " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);

        return;
    }

    @Override
    public String toString() {
        String s = new String();
        s.concat(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID.getVal() + " " + getTopologyConfigId() +" ");
        s.concat(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED.getVal() + " " + getLastSnapStartTimestamp() +" ");
        s.concat(LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED.getVal() + " " + getLastSnapTransferDoneTimestamp() + " ");
        s.concat(LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED.getVal() + " " + getLastSrcBaseSnapshotTimestamp() + " ");
        s.concat(LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM.getVal() + " " + getLastSnapSeqNum() + " ");
        s.concat(LogReplicationMetadataType.LAST_LOG_PROCESSED.getVal() + " " + getLastProcessedLogTimestamp() + " ");

        return s;
    }

    public static String getPersistedWriterMetadataTableName(String localClusterId) {
        return TABLE_PREFIX_NAME + localClusterId;
    }

    public long getLogHead() {
        return runtime.getAddressSpaceView().getTrimMark().getSequence();
    }

    public enum LogReplicationMetadataType {
        TOPOLOGY_CONFIG_ID("topologyConfigId"),
        VERSION("version"),
        LAST_SNAPSHOT_STARTED("lastSnapStart"),
        LAST_SNAPSHOT_TRANSFERRED("lastSnapTransferred"),
        LAST_SNAPSHOT_APPLIED("lastSnapApplied"),
        LAST_SNAPSHOT_SEQ_NUM("lastSnapSeqNum"),
        LAST_LOG_PROCESSED("lastLogProcessed");

        @Getter
        String val;
        LogReplicationMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
