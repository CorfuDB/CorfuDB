package org.corfudb.universe.scenario.fixture;

import com.google.common.collect.ImmutableList;
import lombok.Data;
import lombok.experimental.Accessors;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.universe.vm.VmUniverseParams;
import org.slf4j.event.Level;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Fixture factory provides predefined fixtures
 */
public interface Fixtures {

    /**
     * Common constants used for test
     */
    class TestFixtureConst {
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

        @Override
        public UniverseParams data() {
            this.servers = FixtureUtil.buildMultipleServers(numNodes, corfuCluster.getName());
            servers.forEach(corfuCluster::add);

            return UniverseParams.universeBuilder()
                    .build()
                    .add(corfuCluster);
        }
    }

    /**
     * Configuration for VM specific Universe initialization
     */
    class VmUniverseFixture extends AbstractUniverseFixture<VmUniverseParams> {
        private static final String VM_PREFIX = "corfu-vm-";

        @Override
        public VmUniverseParams data() {
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

            return params;
        }
    }

    @Data
    @Accessors(chain = true)
    abstract class AbstractUniverseFixture<T extends UniverseParams> implements Fixture<T> {
        protected int numNodes;
        protected ClientParams client;
        protected CorfuClusterParams corfuCluster;
        protected ImmutableList<CorfuServerParams> servers;

        public AbstractUniverseFixture() {
            this.numNodes = 3;
            this.client = ClientParams.builder().build();
            this.corfuCluster = CorfuClusterParams.builder().build();
        }
    }

    class FixtureUtil {

        static ImmutableList<CorfuServerParams> buildMultipleServers(int numNodes, String clusterName) {
            List<CorfuServerParams> serversParams = new ArrayList<>();

            for (int i = 0; i < numNodes; i++) {
                final int port = 9000 + i;

                CorfuServerParams serverParam = CorfuServerParams.serverParamsBuilder()
                        .port(port)
                        .clusterName(clusterName)
                        .build();

                serversParams.add(serverParam);
            }
            return ImmutableList.copyOf(serversParams);
        }

        static ImmutableList<CorfuServerParams> buildVmMultipleServers(int numNodes, String clusterName, String vmNamePrefix) {
            List<CorfuServerParams> serversParams = new ArrayList<>();

            for (int i = 0; i < numNodes; i++) {
                final int port = 9000 + i;

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
