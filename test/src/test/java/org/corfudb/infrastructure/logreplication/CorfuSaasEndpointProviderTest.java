package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.utils.CorfuSaasEndpointProvider;
import org.junit.Test;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuSaasEndpointProviderTest {

    private final String pluginConfigFile = "src/test/resources/transport/grpcConfig.properties";

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
}
