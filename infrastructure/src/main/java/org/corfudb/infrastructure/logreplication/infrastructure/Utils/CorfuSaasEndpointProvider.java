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

    @Getter
    private final String configFile;

    @Getter
    private final Optional<String> corfuSaasEndpoint;

    private CorfuSaasEndpointProvider(String configFile) {
        this.configFile = configFile;
        this.corfuSaasEndpoint = getEndpoint(configFile);
    }

    public static void init(String pluginConfigFile) {
        instance = Optional.of(new CorfuSaasEndpointProvider(pluginConfigFile));
    }

    private Optional<String> getEndpoint(String pluginConfigFilePath) {
        if (pluginConfigFilePath != null) {
            return extractEndpoint(pluginConfigFilePath);
        }
        log.warn("No plugin path found");
        return Optional.empty();
    }

    private Optional<String> extractEndpoint(String path) {
        try (InputStream input = new FileInputStream(path)) {
            Properties prop = new Properties();
            prop.load(input);
            String endpoint = prop.getProperty("saas_endpoint");
            return Optional.ofNullable(endpoint);
        } catch (IOException e) {
            log.warn("Error extracting saas endpoint");
            return Optional.empty();
        }
    }

    public static Optional<String> getCorfuSaasEndpoint() {
        return instance.flatMap(i -> i.corfuSaasEndpoint);
    }
}
