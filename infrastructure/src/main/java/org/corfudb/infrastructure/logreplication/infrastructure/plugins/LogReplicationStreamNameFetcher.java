package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import java.util.Map;

public interface LogReplicationStreamNameFetcher {

    Map<String, String> fetchStreamsToReplicate();

    String getVersion();
}
