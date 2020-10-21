package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import java.util.HashMap;
import java.util.Map;

/**
 * Default testing implementation of a Log Replication Config Provider
 *
 * This implementation retrieves a fixed set of tables, which are used for testing purposes.
 */
public class DefaultLogReplicationConfigAdapter implements ILogReplicationConfigAdapter {

    private Map<String, String> streamsNamespaceMap;

    private final int totalMapCount = 10;
    private static final String TABLE_PREFIX = "Table00";
    private static final String NAMESPACE = "LR-Test";

    public DefaultLogReplicationConfigAdapter() {
        streamsNamespaceMap = new HashMap<>();
        streamsNamespaceMap.put("Table001", NAMESPACE);
        streamsNamespaceMap.put("Table002", NAMESPACE);
        streamsNamespaceMap.put("Table003", NAMESPACE);

        // Support for UFO
        for(int i=1; i<=totalMapCount; i++) {
            streamsNamespaceMap.put(NAMESPACE + "$" + TABLE_PREFIX + i, NAMESPACE);
        }
    }

    @Override
    public Map<String, String> fetchStreamsToReplicate() {
        return streamsNamespaceMap;
    }

    @Override
    public String getVersion() {
        return "version_latest";
    }
}
