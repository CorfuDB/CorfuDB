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

    public DefaultLogReplicationConfigAdapter() {
        streamsNamespaceMap = new HashMap<>();
        streamsNamespaceMap.put("Table001", "testNamespace");
        streamsNamespaceMap.put("Table002", "testNamespace");
        streamsNamespaceMap.put("Table003", "testNamespace");
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
