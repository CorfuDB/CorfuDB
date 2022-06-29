package org.corfudb.wavefront;


import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.wavefront.WavefrontConfig;
import io.micrometer.wavefront.WavefrontMeterRegistry;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.common.metrics.micrometer.registries.RegistryProvider;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

public class WavefrontRegistryProvider implements RegistryProvider {
    private Optional<MeterRegistry> registry = Optional.empty();

    @Builder(toBuilder = true)
    @Getter
    @ToString
    public static class WavefrontRegistryConfig {
        private final boolean enabled;
        @NonNull
        private String apiToken;
        @NonNull
        private final String source;
        @NonNull
        private final String prefix;
        @NonNull
        private final String proxyHost;

        private final int proxyPort;

        private final int exportDurationSeconds;

        public static WavefrontRegistryConfig getConfig() throws IOException {
            Properties props = new Properties();
            InputStream resourceAsStream = WavefrontRegistryConfig.class.getClassLoader().getResourceAsStream("config_location.properties");
            if (resourceAsStream != null) {
                props.load(resourceAsStream);
            } else {
                throw new FileNotFoundException("config_location.properties file is not present");
            }
            String configLocation = Optional.ofNullable(props.getProperty("config_location"))
                    .orElseThrow(() ->
                            new IllegalStateException("config_location field must be present"));
            if (!Files.exists(Paths.get(configLocation))) {
                throw new FileNotFoundException(configLocation + " is not present");
            }
            try (FileInputStream fileInputStream = new FileInputStream(configLocation)) {
                Properties configProps = new Properties();
                configProps.load(fileInputStream);
                boolean enabled = Boolean.parseBoolean(configProps.getProperty("enabled", "false"));
                String apiToken = configProps.getProperty("proxy_api_token", "token");
                String source = InetAddress.getLocalHost().getHostName();
                String prefix = configProps.getProperty("prefix", "corfu");
                String proxyHost = configProps.getProperty("proxy_host", "localhost");
                int proxyPort = Integer.parseInt(configProps.getProperty("proxy_port", "2878"));
                int exportDurationSeconds = Integer.parseInt(configProps.getProperty("export_duration_seconds", "60"));
                return WavefrontRegistryConfig.builder()
                        .enabled(enabled)
                        .apiToken(apiToken)
                        .source(source)
                        .prefix(prefix)
                        .proxyHost(proxyHost)
                        .proxyPort(proxyPort)
                        .exportDurationSeconds(exportDurationSeconds)
                        .build();
            }
        }
    }

    @Override
    public MeterRegistry provideRegistry() {

        try {
            WavefrontRegistryConfig config = WavefrontRegistryConfig.getConfig();
            if (!config.isEnabled()) {
                throw new IllegalStateException("Wavefront registry is not enabled.");
            }
            WavefrontConfig wavefrontConfig = new WavefrontConfig() {
                @Override
                public String uri() {
                    return String.format("proxy://%s:%s", config.proxyHost, config.proxyPort);
                }

                @Override
                public Duration step() {
                    return Duration.ofSeconds(config.exportDurationSeconds);
                }

                @Override
                public String prefix() {
                    return config.prefix;
                }

                @Override
                public String source() {
                    return config.source;
                }

                @Override
                public String apiToken() {
                    return config.apiToken;
                }

                @Override
                public String get(String s) {
                    return null;
                }
            };

            registry = Optional.of(new WavefrontMeterRegistry(wavefrontConfig, Clock.SYSTEM));
            return registry.get();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        registry.ifPresent(MeterRegistry::close);
    }

}