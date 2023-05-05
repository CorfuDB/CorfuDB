package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.Utils.CorfuSaasEndpointProvider;
import org.junit.Test;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuSaasEndpointProviderTest {

    /**
     * Uses a config file which has the property "saas_endpoint".
     * This test call the utility function and expects the value from the config file
     */
    @Test
    public void TestGetCorfuEndpoint_SaasEndpointPresent() {
        final String PLUGIN_CONFIG_FILE_WITH_SAAS_ENDPOINT = "src/test/resources/transport/grpcConfig.properties";
        CorfuSaasEndpointProvider.init(PLUGIN_CONFIG_FILE_WITH_SAAS_ENDPOINT);
        Optional<String> endpoint = CorfuSaasEndpointProvider.getCorfuSaasEndpoint();
        assertThat(endpoint.isPresent()).isTrue();
        assertThat(endpoint.get()).isEqualTo("9000");
    }

    /**
     * Uses a config file which does NOT have the property "saas_endpoint".
     * This test calls the utility function, and expects for an empty optional obj.
     */
    @Test
    public void TestGetCorfuEndpoint_withoutSaasEndpoint() {
        final String PLUGIN_CONFIG_FILE_WITHOUT_SAAS_ENDPOINT = "src/test/resources/transport/grpcConfigUpgradeSource.properties";
        CorfuSaasEndpointProvider.init(PLUGIN_CONFIG_FILE_WITHOUT_SAAS_ENDPOINT);
        Optional<String> endpoint = CorfuSaasEndpointProvider.getCorfuSaasEndpoint();
        assertThat(endpoint.isPresent()).isFalse();
    }
}
