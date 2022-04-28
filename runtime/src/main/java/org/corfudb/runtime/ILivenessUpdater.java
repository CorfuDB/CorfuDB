package org.corfudb.runtime;

public interface ILivenessUpdater {
    void updateLiveness(CorfuStoreMetadata.TableName tableName);
    void notifyOnSyncComplete();
}
