package org.corfudb.runtime;

public interface LivenessUpdater {
    void updateLiveness(CorfuStoreMetadata.TableName tableName);

    void notifyOnSyncComplete();

    void shutdown();
}
