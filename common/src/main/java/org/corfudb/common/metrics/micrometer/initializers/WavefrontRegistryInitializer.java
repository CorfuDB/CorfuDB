package org.corfudb.common.metrics.micrometer.initializers;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.wavefront.WavefrontConfig;
import io.micrometer.wavefront.WavefrontMeterRegistry;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * A configuration class for creating a WavefrontMeterRegistry.
 * Defines the location of Wavefront proxy as well as the exporting frequency.
 */
@Builder
@ToString
@Getter
public class WavefrontRegistryInitializer implements RegistryInitializer {
    @Default
    private final String host = "localhost";

    @Default
    private final int port = 2878;

    @Default
    @NonNull
    private final String apiToken = "";

    @Default
    private final Duration exportDuration = Duration.ofMinutes(1);

    @Default
    @NonNull
    private final String source = "localhost";

    @Default
    @NonNull
    private final String prefix = "corfu";

    @Override
    public MeterRegistry createRegistry() {
        Supplier<WavefrontConfig> wavefrontConfigSupplier = () -> new WavefrontConfig() {
            @Override
            public String uri() {
                return String.format("proxy://%s:%s", host, port);
            }

            @Override
            public String get(String key) {
                return null;
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
            public Duration step() {
                return exportDuration;
            }

            @Override
            public String prefix() {
                return prefix;
            }
        };
        return new WavefrontMeterRegistry(wavefrontConfigSupplier.get(), Clock.SYSTEM);
    }
}
