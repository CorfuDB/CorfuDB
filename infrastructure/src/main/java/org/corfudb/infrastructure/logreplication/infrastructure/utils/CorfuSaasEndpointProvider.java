package org.corfudb.infrastructure.logreplication.infrastructure.utils;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper methods to retrieve the corfu's SaaS endpoint.
 */
@Slf4j
public final class CorfuSaasEndpointProvider {
    private static CorfuSaasEndpointProvider instance;
    
    private final Optional<String> corfuSaasEndpoint;

    // e.g. log-replication2-2.log-replication2.default.svc.cluster.local
    private String lrCloudEndpoint;

    private final boolean isSaasDeployment;

    private CorfuSaasEndpointProvider(String configFile, boolean isSaasDeployment) {
        this.isSaasDeployment = isSaasDeployment;
        this.corfuSaasEndpoint = getCorfuSaasEndpointFromConfig(configFile);
    }

    /**
     * Overloaded constructor used for cloud test deployments.
     */
    private CorfuSaasEndpointProvider(boolean isSaasDeployment) {
        this.isSaasDeployment = isSaasDeployment;
        lrCloudEndpoint = extractLocalNodeId();
        this.corfuSaasEndpoint = getCorfuCloudEndpoint();
    }

    public static void init(String pluginConfigFile, boolean isSaasDeployment) {
        instance = new CorfuSaasEndpointProvider(pluginConfigFile, isSaasDeployment);
    }

    public static void init(boolean isSaasDeployment) {
        instance = new CorfuSaasEndpointProvider(isSaasDeployment);
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

    private Optional<String> getCorfuCloudEndpoint() {
        // if not SaaS deployment, return an empty optional
        if(!isSaasDeployment) {
            log.debug("Not a SaaS deployment");
            return Optional.empty();
        }
        return Optional.of(buildCorfuCloudEndpoint(lrCloudEndpoint));
    }

    /**
     * Construct local cloud corfu endpoint from lr cloud endpoint
     */
   @VisibleForTesting
    public static String buildCorfuCloudEndpoint(String lrCloudEndpoint) {
        String name = lrCloudEndpoint.split("\\.")[0];
        String suffix = "";
        if (!name.split("-")[1].matches("[a-zA-Z]+")) {
            suffix = "2";
        }
        return "corfu" + suffix + "-" + name.split("-")[2] + ".corfu" + suffix + "-headless.default.svc.cluster.local:9000";
    }

    /**
     * Extract local node ID from etc/hosts file
     * @return node's dns alias in a cloud deployment
     */
    public static String extractLocalNodeId() {
        return constructLocalNodeId(null);
    }

    /**
     * Extract local node ID from provided file content
     * @return node's dns alias in a cloud deployment
     */
    @VisibleForTesting
    public static String constructLocalNodeId(String content) {
        String nodeId = "";
        String hostFilePath = "/etc/hosts";

        if (content == null) {
            try {
                content = new String(Files.readAllBytes(Paths.get(hostFilePath)), StandardCharsets.UTF_8);
            } catch (IOException e) {
                log.warn("No nodeId Extracted!");
                return nodeId;
            }
        }

        Pattern pattern = Pattern.compile("log-.*local");
        Matcher matcher = pattern.matcher(content);

        if(matcher.find()) {
            nodeId = matcher.group();
        } else {
            log.warn("LR node dns alias not found in {}", hostFilePath);
        }

        log.info("Extracted nodeId: {}", nodeId);
        return nodeId;
    }

    /**
     * Get SaaS endpoint for corfu
     * @return endpoint when deployment is saas, otherwise return null
     */
    public static Optional<String> getCorfuSaasEndpoint() {
        return instance.corfuSaasEndpoint;
    }

    /**
     * Get cloud endpoint for lr
     * @return endpoint when deployment is saas, otherwise return null
     */
    public static String getLrCloudEndpoint() {
        return instance.lrCloudEndpoint;
    }
}
