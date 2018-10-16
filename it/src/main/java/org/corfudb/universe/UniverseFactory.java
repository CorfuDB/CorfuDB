package org.corfudb.universe;

import com.spotify.docker.client.DockerClient;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.universe.docker.DockerUniverse;
import org.corfudb.universe.universe.process.ProcessUniverse;
import org.corfudb.universe.universe.vm.ApplianceManager;
import org.corfudb.universe.universe.vm.VmUniverse;
import org.corfudb.universe.universe.vm.VmUniverseParams;

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
     * @param universeParams {@link Universe} parameters
     * @param docker         docker client.
     * @return a instance of {@link DockerUniverse}
     */
    public DockerUniverse buildDockerUniverse(UniverseParams universeParams, DockerClient docker,
                                              LoggingParams loggingParams) {
        return DockerUniverse.builder()
                .universeParams(universeParams)
                .loggingParams(loggingParams)
                .docker(docker)
                .build();
    }

    /**
     * Build a VM {@link Universe} based on provided {@link VmUniverseParams}.
     *
     * @param universeParams {@link VmUniverse} parameters
     * @param applianceManager      appliances manager.
     * @return a instance of {@link VmUniverse}
     */
    public VmUniverse buildVmUniverse(VmUniverseParams universeParams, ApplianceManager applianceManager) {
        return VmUniverse.builder()
                .universeParams(universeParams)
                .applianceManager(applianceManager)
                .build();
    }

    public ProcessUniverse buildProcessUniverse(UniverseParams universeParams) {
        throw new UnsupportedOperationException();
    }


}

