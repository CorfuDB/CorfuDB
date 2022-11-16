package org.corfudb.infrastructure.logreplication.replication.fsm;

public abstract class AbstractLogReplicationFSM {

    abstract void initializeStates(SnapshotSender snapshotSender, LogEntrySender logEntrySender, DataSender dataSender);
}
