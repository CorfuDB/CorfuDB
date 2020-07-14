package org.corfudb.infrastructure.logreplication;

public interface LogReplicationConfigUpdateListener {

    void onLogReplicationConfigUpdate(LogReplicationConfig config);
}
