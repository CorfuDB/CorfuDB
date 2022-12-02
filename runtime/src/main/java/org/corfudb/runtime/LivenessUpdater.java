package org.corfudb.runtime;

public interface LivenessUpdater {
    void start();

    void updateLiveness(CorfuStoreMetadata.TableName tableName);

    void notifyOnSyncComplete();

    void shutdown();
}
