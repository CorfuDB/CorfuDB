package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import java.util.HashSet;
import java.util.Set;

/**
 * Default testing implementation of a Log Replication Config Provider
 *
 * This implementation retrieves a fixed set of tables, which are used for testing purposes.
 */
public class DefaultLogReplicationConfigAdapter implements ILogReplicationConfigAdapter {

    private Set<String> streamsToReplicate;

    private final int totalMapCount = 10;
    private static final String TABLE_PREFIX = "Table00";
    private static final String NAMESPACE = "LR-Test";

    public DefaultLogReplicationConfigAdapter() {
        streamsToReplicate = new HashSet<>();
        streamsToReplicate.add("Table001");
        streamsToReplicate.add("Table002");
        streamsToReplicate.add("Table003");

        // Support for UFO
        for(int i=1; i<=totalMapCount; i++) {
            streamsToReplicate.add(NAMESPACE + "$" + TABLE_PREFIX + i);
        }
    }

    // Provides the fully qualified names of streams to replicate
    @Override
    public Set<String> fetchStreamsToReplicate() {
        return streamsToReplicate;
    }

    @Override
    public String getVersion() {
        return "version_latest";
    }
}
