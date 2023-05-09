package org.corfudb.infrastructure.logreplication.infrastructure.Utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

/**
 * Helper methods to retrieve the corfu's SaaS endpoint.
 */
@Slf4j
public class CorfuSaasEndpointProvider {
    private static Optional<CorfuSaasEndpointProvider> instance = Optional.empty();
    
    private final Optional<String> corfuSaasEndpoint;

    private CorfuSaasEndpointProvider(String configFile) {
        this.corfuSaasEndpoint = getCorfuSaasEndpoint(configFile);
    }

    public static void init(String pluginConfigFile) {
        instance = Optional.of(new CorfuSaasEndpointProvider(pluginConfigFile));
    }

    private Optional<String> getCorfuSaasEndpoint(String pluginConfigFilePath) {
        if (pluginConfigFilePath != null) {
            return extractSaasEndpoint(pluginConfigFilePath);
        }
        log.warn("No plugin path found");
        return Optional.empty();
    }

    private Optional<String> extractSaasEndpoint(String path) {
        try (InputStream input = new FileInputStream(path)) {
            Properties prop = new Properties();
            prop.load(input);
            String endpoint = prop.getProperty("saas_endpoint");
            return Optional.ofNullable(endpoint);
        } catch (IOException e) {
            log.warn("Error extracting corfu saas endpoint");
            return Optional.empty();
        }
    }

    public static Optional<String> getCorfuSaasEndpoint() {
        return instance.flatMap(i -> i.corfuSaasEndpoint);
    }
}
