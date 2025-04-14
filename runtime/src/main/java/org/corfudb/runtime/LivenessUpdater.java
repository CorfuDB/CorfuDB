package org.corfudb.runtime;

public interface LivenessUpdater {
    void start();

    void setCurrentTable(CorfuStoreMetadata.TableName tableName);

    void unsetCurrentTable();

    void shutdown();
}
