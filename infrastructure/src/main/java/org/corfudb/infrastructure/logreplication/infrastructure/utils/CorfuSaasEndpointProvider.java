package org.corfudb.infrastructure.logreplication.infrastructure.utils;

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
public final class CorfuSaasEndpointProvider {
    private static CorfuSaasEndpointProvider instance;
    
    private final Optional<String> corfuSaasEndpoint;

    private final boolean isSaasDeployment;

    private CorfuSaasEndpointProvider(String configFile, boolean isSaasDeployment) {
        this.isSaasDeployment = isSaasDeployment;
        this.corfuSaasEndpoint = getCorfuSaasEndpointFromConfig(configFile);
    }

    public static void init(String pluginConfigFile, boolean isSaasDeployment) {
        instance = new CorfuSaasEndpointProvider(pluginConfigFile, isSaasDeployment);
    }

    private Optional<String> getCorfuSaasEndpointFromConfig(String pluginConfigFilePath) {
        // if not SaaS deployment, return an empty optional
        if(!isSaasDeployment) {
            log.debug("Not a SaaS deployment");
            return Optional.empty();
        }

        if (pluginConfigFilePath != null) {
            return extractSaasEndpoint(pluginConfigFilePath);
        }
        log.warn("plugin config file not found {}", pluginConfigFilePath);
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

    /**
     * Get SaaS endpoint for corfu
     * @return endpoint when deployment is saas, otherwise return null
     */
    public static Optional<String> getCorfuSaasEndpoint() {
        return instance.corfuSaasEndpoint;
    }
}
