package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;

import java.util.Map;
import java.util.Set;

public interface IConnectionModelPlugin {

    //On startup and on topology change (add/remove endpoints)
    Set<LogReplicationClusterInfo.ClusterConfigurationMsg> getConnectionEndpoints();

    // to use nsxRpc or grpc
    Map<LogReplicationClusterInfo.ClusterConfigurationMsg, String> getRpcPlugin();
}
