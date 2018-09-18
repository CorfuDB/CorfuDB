package org.corfudb.universe;

import com.spotify.docker.client.DockerClient;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.docker.DockerUniverse;
import org.corfudb.universe.universe.process.ProcessUniverse;
import org.corfudb.universe.universe.vm.VmUniverse;

import static org.corfudb.universe.universe.Universe.UniverseParams;

/**
 * {@link UniverseFactory} is a factory for creating different types of {@link Universe}s including:
 * Docker, VM, and Process clusters.
 */
public class UniverseFactory {
    private static final UniverseFactory SINGLETON = new UniverseFactory();

    private UniverseFactory() {
        // To prevent instantiation.
    }

    public static UniverseFactory getInstance() {
        return SINGLETON;
    }

    /**
     * Build a docker {@link Universe} based on provided {@link UniverseParams} and
     * {@link DockerClient}.
     *
     * @param universeParams {@link Universe parameters
     * @param docker        docker client.
     * @return a instance of {@link DockerUniverse}
     */
    public DockerUniverse buildDockerCluster(UniverseParams universeParams, DockerClient docker) {
        return DockerUniverse.builder()
                .universeParams(universeParams)
                .docker(docker)
                .build();
    }

    public VmUniverse buildVmCluster(UniverseParams universeParams) {
        throw new UnsupportedOperationException();
    }

    public ProcessUniverse buildProcessCluster(UniverseParams universeParams) {
        throw new UnsupportedOperationException();
    }


}

