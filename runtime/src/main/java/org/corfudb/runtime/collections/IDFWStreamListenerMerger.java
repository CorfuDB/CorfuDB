package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import org.corfudb.infrastructure.logreplication.proto.ReplicationStatusVal;

import java.util.List;
import java.util.Map;

public interface IDFWStreamListenerMerger extends LrMultiStreamMergeStreamListener{

    CorfuStore store = null;
    String metadataNamespace = null;
    String metadataStreamTag = null;
    String appNamespace = null;
    String appStreamTag = null;
    boolean isFullSyncInProgress = false;
    Table<Message, Message, Message> idfwMergedTable = null;
    public static final String IDFW_TABLE = "IDFW_TABLE";
    public static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";

    public IDFWStreamListenerMerger(CorfuStore store, String metadataNamespace, String metadataStreamTag, String appNamespace, String appStreamTag) {
        this.store = store;
        this.metadataNamespace = metadataNamespace;
        this.metadataStreamTag = metadataStreamTag;
        this.appNamespace = appNamespace;
        this.appStreamTag = appStreamTag;

        // IDFW proto files need to be added!
        this.idfwMergedTable = store.openTable(appNamespace, "IDFW_Merged_Table",
                IDFWKey.class, IDFWValue.class, IDFWMetadata.class, TableOptions.builder().build());

        this.isFullSyncInProgress = false;
    }

    @Override
    public default void onNext(CorfuStreamEntries results) {

        Map<TableSchema, List<CorfuStreamEntry>> entries = results.getEntries();

        for (TableSchema tableSchema : entries.keySet()) {

            if (tableSchema.getTableName() == REPLICATION_STATUS_TABLE) {

                for (CorfuStreamEntry entry: entries.get(tableSchema)) {
                    if ((ReplicationStatusVal)entry.getPayload().getIsDataConsistent() == false) {
                        // Flag to know that we are in full sync in progress
                        this.isFullSyncInProgress = true;
                    } else {
                        this.isFullSyncInProgress = false;
                        // Full Sync is COMPLETED, determine deleted keys based off in-memory keySet maintained during full sync in progress
                        // this includes determine stale entries for tables that were not replicated at all...
                        // Example earlier state: [k1, k2, k3 (GT1)] - [k5, k6, K7, k8 GT2]
                        // After full sync: [k5, k6, k8] -> it will also clear the keys for destinations removed from a logical group (domain)
                        determineDeletes();
                    }
                }
            } else if (tableSchema.getTableName() == IDFW_TABLE) {
                for (CorfuStreamEntry entry: entries.get(tableSchema)) {
                    if ((entry.getOperation().equals(CorfuStreamEntry.OperationType.CLEAR)) && (!isFullSyncInProgress)) {
                        idfwMergedTable.clearAll();
                    } else if (entry.getOperation().equals(CorfuStreamEntry.OperationType.UPDATE)) {
                        idfwMergedTable.put(entry.getKey(), entry.getPayload(), entry.getMetadata());
                    } else if (entry.getOperation().equals(CorfuStreamEntry.OperationType.DELETE)) {
                        idfwMergedTable.deleteRecord(entry.getKey());
                    }
                }
            } else {
                System.out.println("UNKNOWN TABLE");
            }
        }
    }

    void determineDeletes();
}
