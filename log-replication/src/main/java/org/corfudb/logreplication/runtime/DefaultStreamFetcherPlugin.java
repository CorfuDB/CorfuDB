package org.corfudb.logreplication.runtime;

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
}
