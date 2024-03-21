package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.utils.CorfuSaasEndpointProvider;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuSaasEndpointProviderTest {

    private final String pluginConfigFile = "src/test/resources/transport/pluginConfig.properties";

    /**
     * This test call the utility function and expects the value from the config file
     */
    @Test
    public void testGetCorfuEndpointSaasDeployment() {
        CorfuSaasEndpointProvider.init(pluginConfigFile, true);
        Optional<String> endpoint = CorfuSaasEndpointProvider.getCorfuSaasEndpoint();
        assertThat(endpoint.isPresent()).isTrue();
        assertThat(endpoint.get()).isEqualTo("corfu:9000");
    }

    /**
     * This test calls the utility function, and expects for an empty optional String obj.
     */
    @Test
    public void testGetCorfuEndpointOnPremDeployment() {
        CorfuSaasEndpointProvider.init(pluginConfigFile, false);
        Optional<String> endpoint = CorfuSaasEndpointProvider.getCorfuSaasEndpoint();
        assertThat(endpoint.isPresent()).isFalse();
    }

    @Test
    public void testConstructLocalNodeId() {
        String randomString = "test /n/n test test ";
        List<String> lrHostAliases = new ArrayList<>(Arrays.asList("log-replication-0.log-replication.default.svc.cluster.local",
                "log-replication2-0.log-replication2.default.svc.cluster.local"));

        for (String host : lrHostAliases) {
            String contentToParse = randomString + host + randomString;
            assertThat(host.equals(CorfuSaasEndpointProvider.constructLocalNodeId(contentToParse))).isTrue();
        }
    }

    @Test
    public void testConstructCorfuCloudEndpoint() {
        List<String> lrHostAliases = new ArrayList<>(Arrays.asList("log-replication-0.log-replication.default.svc.cluster.local",
                "log-replication2-0.log-replication2.default.svc.cluster.local"));
        List<String> expectedCorfuEndpoints = new ArrayList<>(Arrays.asList("corfu-0.corfu-headless.default.svc.cluster.local:9000",
                "corfu2-0.corfu2-headless.default.svc.cluster.local:9000"));

        lrHostAliases = lrHostAliases.stream().map(CorfuSaasEndpointProvider::buildCorfuCloudEndpoint).collect(Collectors.toList());
        assertThat(lrHostAliases.equals(expectedCorfuEndpoints)).isTrue();
    }
}
