package org.corfudb.infrastructure.logreplication.replication.send;

import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;

public interface LRClientEventListener extends StreamListener {

    @Override
    void onNext(CorfuStreamEntries entries);

    @Override
    void onError(Throwable t);

    void registerFsm(LogReplicationFSM fsm);

    void unregisterFsm(LogReplicationFSM fsm);
}
