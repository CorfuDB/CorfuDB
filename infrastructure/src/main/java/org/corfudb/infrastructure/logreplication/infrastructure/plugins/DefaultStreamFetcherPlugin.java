package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationStreamNameFetcher;

import java.util.HashMap;
import java.util.Map;

public class DefaultStreamFetcherPlugin implements LogReplicationStreamNameFetcher {

    private Map<String, String> streamsNamespaceMap;

    public DefaultStreamFetcherPlugin() {
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
