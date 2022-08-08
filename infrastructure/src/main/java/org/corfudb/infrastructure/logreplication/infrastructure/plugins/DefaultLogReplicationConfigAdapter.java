package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

/**
 * Default testing implementation of a Log Replication Config Provider
 *
 * This implementation retrieves a fixed set of tables, which are used for testing purposes.
 */
public class DefaultLogReplicationConfigAdapter implements ILogReplicationVersionAdapter {

    @Override
    public String getVersion() {
        return "version_latest";
    }
}
