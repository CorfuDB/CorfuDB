package org.corfudb.universe.node.server;

import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.Node.NodeType;
import org.corfudb.universe.node.server.CorfuServer.Mode;
import org.corfudb.universe.node.server.CorfuServer.Persistence;
import org.slf4j.event.Level;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;

@Builder(builderMethodName = "serverParamsBuilder")
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public class CorfuServerParams implements NodeParams {
    public static final String DOCKER_IMAGE_NAME = "corfudb-universe/corfu-server";

    @NonNull
    private final String streamLogDir = "db";

    @Default
    private final int port = ServerUtil.getRandomOpenPort();

    @Default
    @NonNull
    private final Mode mode = Mode.CLUSTER;

    @Default
    @NonNull
    private final Persistence persistence = Persistence.DISK;

    @Default
    @NonNull
    @EqualsAndHashCode.Exclude
    private final Level logLevel = Level.INFO;

    @NonNull
    private final NodeType nodeType = NodeType.CORFU_SERVER;

    /**
     * A name of the Corfu cluster
     */
    @NonNull
    private final String clusterName;

    @Default
    @NonNull
    @EqualsAndHashCode.Exclude
    private final Duration stopTimeout = Duration.ofSeconds(1);

    @Default
    private final Optional<ContainerResources> containerResources = Optional.empty();

    /**
     * Corfu server version, for instance: 0.3.0-SNAPSHOT
     */
    @NonNull
    private final String serverVersion;

    @NonNull
    @Default
    private final Path serverJarDirectory = Paths.get("target");

    @NonNull
    @Default
    private final String dockerImage = DOCKER_IMAGE_NAME;

    @Default
    private final double logSizeQuotaPercentage = 100;

    @Override
    public String getName() {
        return clusterName + "-corfu-node" + getPort();
    }

    public String getStreamLogDir() {
        return getName() + "/" + streamLogDir;
    }

    @Override
    public Set<Integer> getPorts() {
        return ImmutableSet.of(port);
    }

    public String getDockerImageNameFullName() {
        return dockerImage + ":" + serverVersion;
    }

    public Path getInfrastructureJar() {
        return serverJarDirectory.resolve(
                String.format("infrastructure-%s-shaded.jar", serverVersion)
        );
    }

    /**
     * https://docs.docker.com/config/containers/resource_constraints/
     */
    @Builder
    @ToString
    public static class ContainerResources {

        /**
         * Memory limit in mb
         */
        @Getter
        @Default
        private final long memory = 1048 * 1024 * 1024;
    }
}
