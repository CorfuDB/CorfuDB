package org.corfudb.universe.group.cluster.docker;

import com.google.common.collect.ImmutableSortedSet;
import com.spotify.docker.client.DockerClient;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.cluster.AbstractSupportCluster;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.group.cluster.SupportClusterParams;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.server.SupportServerParams;
import org.corfudb.universe.node.server.docker.DockerSupportServer;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.util.DockerManager;

/**
 * Provides Docker implementation of {@link CorfuCluster}.
 */
@Slf4j
public class DockerSupportCluster extends AbstractSupportCluster {
    @NonNull
    private final DockerClient docker;
    @NonNull
    private final LoggingParams loggingParams;
    @NonNull
    private final DockerManager dockerManager;

    @Builder
    public DockerSupportCluster(DockerClient docker, SupportClusterParams monitoringParams,
                                UniverseParams universeParams,
                                LoggingParams loggingParams) {
        super(universeParams, monitoringParams);
        this.docker = docker;
        this.loggingParams = loggingParams;
        this.dockerManager = DockerManager.builder().docker(docker).build();
    }


    @Override
    public void bootstrap() {
        // NOOP
    }

    @Override
    protected Node buildServer(SupportServerParams nodeParams) {
        return DockerSupportServer.builder()
                .universeParams(universeParams)
                .clusterParams(params)
                .params(nodeParams)
                .docker(docker)
                .dockerManager(dockerManager)
                .build();
    }
}
