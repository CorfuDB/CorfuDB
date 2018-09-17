package org.corfudb.universe;

import com.spotify.docker.client.DockerClient;
import org.corfudb.universe.cluster.docker.DockerCluster;
import org.corfudb.universe.cluster.process.ProcessCluster;
import org.corfudb.universe.cluster.vm.VmCluster;

import static org.corfudb.universe.cluster.Cluster.ClusterParams;

/**
 * Universe is a factory for creating different types of {@link org.corfudb.universe.cluster.Cluster}s including:
 * Docker, VM, and Process clusters.
 */
public class Universe {
    private static final Universe SINGLETON = new Universe();

    private Universe() {
        // To prevent instantiation.
    }

    public static Universe getInstance() {
        return SINGLETON;
    }

    /**
     * Build a docker {@link org.corfudb.universe.cluster.Cluster} based on provided {@link ClusterParams} and
     * {@link DockerClient}.
     *
     * @param clusterParams cluster parameters
     * @param docker        docker client.
     * @return a instance of {@link DockerCluster}
     */
    public DockerCluster buildDockerCluster(ClusterParams clusterParams, DockerClient docker) {
        return DockerCluster.builder()
                .clusterParams(clusterParams)
                .docker(docker)
                .build();
    }

    public VmCluster buildVmCluster(ClusterParams clusterParams) {
        throw new UnsupportedOperationException();
    }

    public ProcessCluster buildProcessCluster(ClusterParams clusterParams) {
        throw new UnsupportedOperationException();
    }


}

