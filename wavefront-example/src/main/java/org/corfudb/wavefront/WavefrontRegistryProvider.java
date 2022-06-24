package org.corfudb.wavefront;


import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.wavefront.WavefrontConfig;
import io.micrometer.wavefront.WavefrontMeterRegistry;
import org.corfudb.common.metrics.micrometer.registries.RegistryProvider;

import java.time.Duration;
import java.util.Optional;

public class WavefrontRegistryProvider implements RegistryProvider {
    private final String host = "20.21.0.95";
    private final int port = 2878;
    private final String apiToken = "";
    private final Duration exportDuration = Duration.ofSeconds(5);
    private final String source = "localhost";
    private final String prefix = "corfu";

    private Optional<MeterRegistry> registry = Optional.empty();

    @Override
    public MeterRegistry provideRegistry() {
        WavefrontConfig wavefrontConfig = new WavefrontConfig() {
            @Override
            public String uri() {
                return String.format("proxy://%s:%s", host, port);
            }

            @Override
            public Duration step() {
                return exportDuration;
            }

            @Override
            public String prefix() {
                return prefix;
            }

            @Override
            public String source() {
                return source;
            }

            @Override
            public String apiToken() {
                return apiToken;
            }

            @Override
            public String get(String s) {
                return null;
            }
        };

        registry = Optional.of(new WavefrontMeterRegistry(wavefrontConfig, Clock.SYSTEM));
        return registry.get();
    }

    @Override
    public void close() {
        registry.ifPresent(MeterRegistry::close);
    }
}