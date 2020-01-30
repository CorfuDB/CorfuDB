package org.corfudb;

public interface LogReplicationAdapter {

    void snapshotSync();

    void  logEntrySync();
}
