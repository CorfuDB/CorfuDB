package org.corfudb;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;

import java.util.Map;

/**
 * CorfuRuntimeHelper has utilities to create and connect a runtime instance.
 * Created by zlokhandwala on 11/5/19.
 */
@Slf4j
public class CorfuRuntimeHelper {

    private CorfuRuntimeHelper() {
    }

    /**
     * Populate corfu runtime parameters.
     *
     * @param opts Optional arguments passed by the user.
     * @return Returns built parameters.
     */
    private static CorfuRuntimeParameters buildParametersFromArgs(Map<String, Object> opts) {
        CorfuRuntimeParameters.CorfuRuntimeParametersBuilder builder = CorfuRuntimeParameters.builder()
                .useFastLoader(false)
                .cacheDisabled(true);

        boolean tlsEnabled = opts.get("--tlsEnabled") != null
                && Boolean.parseBoolean(opts.get("--tlsEnabled").toString());
        if (tlsEnabled) {
            String keyStore = opts.get("--keystore").toString();
            String ksPasswordFile = opts.get("--ks_password").toString();
            String trustStore = opts.get("--truststore").toString();
            String tsPasswordFile = opts.get("--truststore_password").toString();

            builder.tlsEnabled(tlsEnabled)
                    .keyStore(keyStore)
                    .ksPasswordFile(ksPasswordFile)
                    .trustStore(trustStore)
                    .tsPasswordFile(tsPasswordFile);
        }

        return builder.build();
    }

    /**
     * Returns a connected instance of CorfuRuntime configured with the parameters specified by the user.
     *
     * @param opts Options passed by the user.
     * @return Connected instance of CorfuRuntime.
     */
    public static CorfuRuntime getRuntimeForArgs(Map<String, Object> opts) {
        String host = opts.get("--host").toString();
        Integer port = Integer.parseInt(opts.get("--port").toString());
        String endpoint = String.format("%s:%d", host, port);

        CorfuRuntime corfuRuntime = CorfuRuntime
                .fromParameters(buildParametersFromArgs(opts))
                .parseConfigurationString(endpoint)
                .connect();

        log.info("Successfully connected to {}", endpoint);

        return corfuRuntime;
    }
}
