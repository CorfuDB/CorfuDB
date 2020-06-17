package org.corfudb.infrastructure;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.cluster.ClusterDescriptor;
import org.corfudb.runtime.RuntimeParameters;

@Data
@SuperBuilder
public class LogReplicationRuntimeParameters extends RuntimeParameters {

    private ClusterDescriptor remoteClusterDescriptor;

    private String localCorfuEndpoint;

    private String localClusterId;

    private LogReplicationConfig replicationConfig;

    private String pluginFilePath;
}
