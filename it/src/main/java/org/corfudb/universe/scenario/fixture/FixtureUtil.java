package org.corfudb.universe.scenario.fixture;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.CorfuServerParams.ContainerResources;
import org.corfudb.universe.node.server.CorfuServerParams.CorfuServerParamsBuilder;
import org.corfudb.universe.node.server.ServerUtil;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmCorfuServerParamsBuilder;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmName;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Dynamically generates a list of corfu server params, based on corfu cluster parameters.
 */
@Builder
@Slf4j
public class FixtureUtil {

    @Default
    private final Optional<Integer> initialPort = Optional.empty();
    private Optional<Integer> currPort;

    /**
     * Generates a list of docker corfu server params
     *
     * @param cluster       corfu cluster
     * @param serverBuilder corfu server builder with predefined parameters
     * @return list of docker corfu server params
     */
    ImmutableList<CorfuServerParams> buildServers(
            CorfuClusterParams<CorfuServerParams> cluster, CorfuServerParamsBuilder serverBuilder) {

        currPort = initialPort;

        List<CorfuServerParams> serversParams = IntStream
                .rangeClosed(1, cluster.getNumNodes())
                .map(i -> getPort())
                .boxed()
                .sorted()
                .map(port -> serverBuilder
                        .port(port)
                        .clusterName(cluster.getName())
                        .containerResources(Optional.of(ContainerResources.builder().build()))
                        .serverVersion(cluster.getServerVersion())
                        .build()
                )
                .collect(Collectors.toList());

        return ImmutableList.copyOf(serversParams);
    }

    /**
     * Generates a list of VMs corfu server params
     *
     * @param cluster             VM corfu cluster
     * @param serverParamsBuilder corfu server builder with predefined parameters
     * @return list of VM corfu server params
     */
    protected ImmutableList<VmCorfuServerParams> buildVmServers(
            CorfuClusterParams<VmCorfuServerParams> cluster,
            VmCorfuServerParamsBuilder serverParamsBuilder, String vmNamePrefix) {

        currPort = initialPort;

        List<VmCorfuServerParams> serversParams = new ArrayList<>();

        for (int i = 0; i < cluster.getNumNodes(); i++) {
            int port = getPort();

            VmName vmName = VmName.builder()
                    .name(vmNamePrefix + (i + 1))
                    .index(i)
                    .build();

            try {
                VmCorfuServerParams serverParam = ((VmCorfuServerParams) serverParamsBuilder
                        .vmName(vmName)
                        // Call the parent's builder methods using reflection as calling from
                        // the child builder object (serverParamsBuilder)
                        // will type cast the builder to the parent class (CorfuServerParamsBuilder)
                        // instead of the child class (VmCorfuServerParamsBuilder).
                        .getClass().getMethod("clusterName", String.class)
                            .invoke(serverParamsBuilder, cluster.getName())
                        .getClass().getMethod("port", Integer.class)
                            .invoke(serverParamsBuilder, port)
                        .getClass().getMethod("serverVersion", String.class)
                            .invoke(serverParamsBuilder, cluster.getServerVersion())
                        .getClass().getMethod("build")
                            .invoke(serverParamsBuilder));
                serversParams.add(serverParam);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                log.error("buildVmServers: Error while building serverParams using reflection: {}", e.getMessage());
            }
        }

        return ImmutableList.copyOf(serversParams);
    }

    private int getPort() {
        currPort = currPort.map(oldPort -> oldPort + 1);
        return currPort.orElseGet(ServerUtil::getRandomOpenPort);
    }
}
