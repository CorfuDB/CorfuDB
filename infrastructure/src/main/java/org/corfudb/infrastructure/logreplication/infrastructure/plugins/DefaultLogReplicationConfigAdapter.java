package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

/**
 * Default testing implementation of a Log Replication Config Provider
 */

public class DefaultLogReplicationConfigAdapter implements ILogReplicationVersionAdapter {

    @Override
    public String getVersion() {
        return "version_latest";
    }
}
