package org.corfudb.runtime;

public interface ILivenessUpdater {
    void notifyOnSyncStart();
    void updateLiveness(CorfuStoreMetadata.TableName tableName);
    void notifyOnSyncComplete();
}
