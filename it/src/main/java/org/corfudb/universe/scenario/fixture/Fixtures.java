package org.corfudb.universe.scenario.fixture;

import com.google.common.collect.ImmutableList;
import lombok.Data;
import lombok.experimental.Accessors;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.group.cluster.SupportClusterParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.CorfuServerParams.ContainerResources;
import org.corfudb.universe.node.server.ServerUtil;
import org.corfudb.universe.node.server.SupportServerParams;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.universe.vm.VmUniverseParams;
import org.slf4j.event.Level;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Fixture factory provides predefined fixtures
 */
public interface Fixtures {

    /**
     * Common constants used for test
     */
    class TestFixtureConst {

        private TestFixtureConst() {
            // prevent instantiation of this class
        }

        // Default name of the CorfuTable created by CorfuClient
        public static final String DEFAULT_STREAM_NAME = "stream";

        // Default number of values written into CorfuTable
        public static final int DEFAULT_TABLE_ITER = 100;

        // Default number of times to poll layout
        public static final int DEFAULT_WAIT_POLL_ITER = 300;

        // Default time to wait before next layout poll: 1 second
        public static final int DEFAULT_WAIT_TIME = 1;
    }

    /**
     * Common configuration for Universe initialization
     */
    class UniverseFixture extends AbstractUniverseFixture<UniverseParams> {
        public UniverseParams data;

        @Override
        public UniverseParams data() {
            if (data != null){
                return data;
            }

            this.servers = FixtureUtil.buildMultipleServers(
                    corfuServerVersion, numNodes, corfuCluster.getName()
            );

            servers.forEach(corfuCluster::add);
            data = UniverseParams.universeBuilder()
                    .build()
                    .add(corfuCluster);

            if (metricsPorts.isEmpty()) {
                return data;
            }

            final SupportServerParams monitoring =
                    SupportServerParams.builder()
                            .clusterName(monitoringCluster.getName())
                            .metricPorts(metricsPorts)
                            .nodeType(Node.NodeType.METRICS_SERVER)
                            .build();

            monitoringCluster.add(monitoring);
            data.add(monitoringCluster);
            return data;
        }
    }

    /**
     * Configuration for VM specific Universe initialization
     */
    class VmUniverseFixture extends AbstractUniverseFixture<VmUniverseParams> {
        private static final String VM_PREFIX = "corfu-vm-";

        public VmUniverseParams data;

        @Override
        public VmUniverseParams data() {
            if (data != null){
                return data;
            }

            this.servers = FixtureUtil.buildVmMultipleServers(numNodes, corfuCluster.getName(), VM_PREFIX);
            servers.forEach(corfuCluster::add);

            ConcurrentMap<String, String> vmIpAddresses = new ConcurrentHashMap<>();
            for (int i = 0; i < numNodes; i++) {
                vmIpAddresses.put(VM_PREFIX + (i + 1), "0.0.0.0");
            }

            Properties credentials = new Properties();
            try (InputStream is = ClassLoader.getSystemResource("vm.properties").openStream()) {
                credentials.load(is);
            } catch (IOException e) {
                throw new IllegalStateException("Can't load credentials", e);
            }

            VmUniverseParams params = VmUniverseParams.builder()
                    .vSphereUrl("https://10.173.65.98/sdk")
                    .vSphereUsername(credentials.getProperty("vsphere.username"))
                    .vSpherePassword(credentials.getProperty("vsphere.password"))
                    .vSphereHost("10.172.208.208")
                    .templateVMName("IntegTestVMTemplate")
                    .vmUserName("vmware")
                    .vmPassword("vmware")
                    .vmIpAddresses(vmIpAddresses)
                    .networkName("corfu_network")
                    .build();
            params.add(corfuCluster);

            data = params;

            return data;
        }
    }

    @Data
    @Accessors(chain = true)
    abstract class AbstractUniverseFixture<T extends UniverseParams> implements Fixture<T> {
        protected String corfuServerVersion;
        protected int numNodes;
        protected Set<Integer> metricsPorts = new HashSet<>();
        protected ClientParams client;
        protected CorfuClusterParams corfuCluster;
        protected SupportClusterParams monitoringCluster;
        protected ImmutableList<CorfuServerParams> servers;

        public AbstractUniverseFixture() {
            this.numNodes = 3;
            this.client = ClientParams.builder().build();
            this.corfuCluster = CorfuClusterParams.builder().build();
            this.monitoringCluster = SupportClusterParams.builder().build();
        }
    }

    class FixtureUtil {

        private FixtureUtil() {
            // prevent instantiation of this class
        }

        static ImmutableList<CorfuServerParams> buildMultipleServers(
                String corfuServerVersion, int numNodes, String clusterName) {

            List<CorfuServerParams> serversParams = IntStream
                    .rangeClosed(1, numNodes)
                    .map(i -> ServerUtil.getRandomOpenPort())
                    .boxed()
                    .sorted()
                    .map(port -> CorfuServerParams.serverParamsBuilder()
                            .port(port)
                            .clusterName(clusterName)
                            //.containerResources(Optional.of(ContainerResources.builder().build()))
                            .serverVersion(corfuServerVersion)
                            .build()
                    )
                    .collect(Collectors.toList());

            return ImmutableList.copyOf(serversParams);
        }

        static ImmutableList<CorfuServerParams> buildVmMultipleServers(int numNodes, String clusterName, String vmNamePrefix) {
            List<CorfuServerParams> serversParams = new ArrayList<>();

            for (int i = 0; i < numNodes; i++) {
                final int port = ServerUtil.getRandomOpenPort();

                VmCorfuServerParams serverParam = VmCorfuServerParams.builder()
                        .clusterName(clusterName)
                        .vmName(vmNamePrefix + (i + 1))
                        .mode(CorfuServer.Mode.CLUSTER)
                        .logLevel(Level.TRACE)
                        .persistence(CorfuServer.Persistence.DISK)
                        .port(port)
                        .stopTimeout(Duration.ofSeconds(1))
                        .build();

                serversParams.add(serverParam);
            }
            return ImmutableList.copyOf(serversParams);
        }
    }
}
