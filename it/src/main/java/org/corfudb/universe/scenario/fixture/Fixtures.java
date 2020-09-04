package org.corfudb.universe.scenario.fixture;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.group.cluster.CorfuClusterParams.CorfuClusterParamsBuilder;
import org.corfudb.universe.group.cluster.SupportClusterParams;
import org.corfudb.universe.group.cluster.SupportClusterParams.SupportClusterParamsBuilder;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.logging.LoggingParams.LoggingParamsBuilder;
import org.corfudb.universe.node.Node.NodeType;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.client.ClientParams.ClientParamsBuilder;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.CorfuServerParams.CorfuServerParamsBuilder;
import org.corfudb.universe.node.server.SupportServerParams;
import org.corfudb.universe.node.server.SupportServerParams.SupportServerParamsBuilder;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmCorfuServerParamsBuilder;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmName;
import org.corfudb.universe.scenario.fixture.FixtureUtil.FixtureUtilBuilder;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.universe.UniverseParams.UniverseParamsBuilder;
import org.corfudb.universe.universe.vm.VmConfigPropertiesLoader;
import org.corfudb.universe.universe.vm.VmUniverseParams;
import org.corfudb.universe.universe.vm.VmUniverseParams.Credentials;
import org.corfudb.universe.universe.vm.VmUniverseParams.VmCredentialsParams;
import org.corfudb.universe.universe.vm.VmUniverseParams.VmUniverseParamsBuilder;
import org.corfudb.universe.util.IpAddress;
import org.slf4j.event.Level;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Fixture factory provides predefined fixtures
 */
public interface Fixtures {

    IpAddress ANY_ADDRESS = IpAddress.builder().ip("0.0.0.0").build();

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
        public static final Duration DEFAULT_WAIT_TIME = Duration.ofSeconds(1);

        private TestFixtureConst() {
            // prevent instantiation of this class
        }
    }

    @Getter
    class UniverseFixture implements Fixture<UniverseParams> {

        private final UniverseParamsBuilder universe = UniverseParams.universeBuilder();

        private final CorfuClusterParamsBuilder<CorfuServerParams> cluster = CorfuClusterParams
                .builder();

        private final CorfuServerParamsBuilder server = CorfuServerParams.serverParamsBuilder();

        private final SupportServerParamsBuilder supportServer = SupportServerParams.builder();

        private final SupportClusterParamsBuilder monitoringCluster = SupportClusterParams
                .builder();

        private final ClientParamsBuilder client = ClientParams.builder();

        private final FixtureUtilBuilder fixtureUtilBuilder = FixtureUtil.builder();

        private final LoggingParamsBuilder logging = LoggingParams.builder()
                .enabled(false);

        private Optional<UniverseParams> data = Optional.empty();

        public UniverseParams data() {

            if (data.isPresent()) {
                return data.get();
            }

            UniverseParams universeParams = universe.build();
            CorfuClusterParams<CorfuServerParams> clusterParams = cluster.build();

            SupportClusterParams monitoringClusterParams = monitoringCluster.build();
            SupportServerParams monitoringServerParams = supportServer
                    .clusterName(monitoringClusterParams.getName())
                    .nodeType(NodeType.METRICS_SERVER)
                    .build();

            FixtureUtil fixtureUtil = fixtureUtilBuilder.build();
            List<CorfuServerParams> serversParams = fixtureUtil.buildServers(
                    clusterParams, server
            );

            serversParams.forEach(clusterParams::add);
            universeParams.add(clusterParams);

            if (monitoringServerParams.isEnabled()) {
                monitoringClusterParams.add(monitoringServerParams);
                universeParams.add(monitoringClusterParams);
            }

            data = Optional.of(universeParams);
            return universeParams;
        }
    }

    @Getter
    class VmUniverseFixture implements Fixture<VmUniverseParams> {
        private static final String DEFAULT_VM_PREFIX = "corfu-vm-";

        private final VmUniverseParamsBuilder universe;

        private final CorfuClusterParamsBuilder<VmCorfuServerParams> cluster = CorfuClusterParams
                .builder();

        private final VmCorfuServerParamsBuilder servers = VmCorfuServerParams.builder();

        private final ClientParamsBuilder client = ClientParams.builder();

        @Setter
        private String vmPrefix = DEFAULT_VM_PREFIX;

        private Optional<VmUniverseParams> data = Optional.empty();

        private final FixtureUtilBuilder fixtureUtilBuilder = FixtureUtil.builder();

        private final Properties vmProperties = VmConfigPropertiesLoader
                .loadVmProperties()
                .get();

        private final LoggingParamsBuilder logging = LoggingParams.builder()
                .enabled(false);

        public VmUniverseFixture() {
            Properties credentials = VmConfigPropertiesLoader
                    .loadVmCredentialsProperties()
                    .orElse(new Properties());

            servers
                    .logLevel(Level.INFO)
                    .mode(CorfuServer.Mode.CLUSTER)
                    .persistence(CorfuServer.Persistence.DISK)
                    .stopTimeout(Duration.ofSeconds(1))
                    .universeDirectory(Paths.get("target"))
                    .dockerImage(CorfuServerParams.DOCKER_IMAGE_NAME)
                    .logSizeQuotaPercentage(100);

            Credentials vSphereCred = Credentials.builder()
                    .username(credentials.getProperty("vsphere.username"))
                    .password(credentials.getProperty("vsphere.password"))
                    .build();

            Credentials vmCred = Credentials.builder()
                    .username(credentials.getProperty("vm.username"))
                    .password(credentials.getProperty("vm.password"))
                    .build();

            VmCredentialsParams credentialParams = VmCredentialsParams.builder()
                    .vSphereCredentials(vSphereCred)
                    .vmCredentials(vmCred)
                    .build();

            List<String> vSphereHost = Arrays
                    .stream(vmProperties.getProperty("vsphere.host").split(","))
                    .collect(Collectors.toList());

            universe = VmUniverseParams.builder()
                    .vSphereUrl(vmProperties.getProperty("vsphere.url"))
                    .vSphereHost(vSphereHost)
                    .networkName(vmProperties.getProperty("vm.network"))
                    .templateVMName(vmProperties.getProperty("vm.template", "debian-buster-thin-provisioned"))
                    .credentials(credentialParams)
                    .cleanUpEnabled(true);
        }

        @Override
        public VmUniverseParams data() {
            if (data.isPresent()) {
                return data.get();
            }

            vmPrefix = vmProperties.getProperty("vm.prefix", vmPrefix);

            CorfuClusterParams<VmCorfuServerParams> clusterParams = cluster.build();

            FixtureUtil fixtureUtil = fixtureUtilBuilder.build();
            ImmutableList<VmCorfuServerParams> serversParams = fixtureUtil.buildVmServers(
                    clusterParams, servers, vmPrefix
            );
            serversParams.forEach(clusterParams::add);

            ConcurrentMap<VmName, IpAddress> vmIpAddresses = new ConcurrentHashMap<>();
            for (int i = 0; i < clusterParams.getNumNodes(); i++) {
                VmName vmName = VmName.builder()
                        .name(vmPrefix + (i + 1))
                        .index(i)
                        .build();
                vmIpAddresses.put(vmName, ANY_ADDRESS);
            }

            VmUniverseParams universeParams = universe
                    .vmIpAddresses(vmIpAddresses)
                    .build();

            universeParams.add(clusterParams);

            data = Optional.of(universeParams);

            return universeParams;
        }
    }
}
