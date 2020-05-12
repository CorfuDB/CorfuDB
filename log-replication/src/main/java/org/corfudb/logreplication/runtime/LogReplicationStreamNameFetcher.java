package org.corfudb.logreplication.runtime;

import java.util.Map;

public interface LogReplicationStreamNameFetcher {

    Map<String, String> fetchStreamsToReplicate();

    String getVersion();
}
