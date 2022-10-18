package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

/**
 * Default testing implementation of a Log Replication Config Provider
 *
 * This implementation retrieves a fixed set of tables, which are used for testing purposes.
 */
public class DefaultLogReplicationConfigAdapter implements ILogReplicationVersionAdapter {
    public static final int MAP_COUNT = 10;
    public static final String TABLE_PREFIX = "Table00";
    public static final String NAMESPACE = "LR-Test";
    public static final String TAG_ONE = "tag_one";

    @Override
    public String getVersion() {
        return "version_latest";
    }
}
