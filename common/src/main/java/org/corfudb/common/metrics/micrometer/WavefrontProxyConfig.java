package org.corfudb.common.metrics.micrometer;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.time.Duration;

/**
 * A configuration class for initializing a WavefrontMeterRegistry.
 * Defines the location of Wavefront proxy as well as the exporting frequency.
 */
@Builder
@ToString
@Getter
public class WavefrontProxyConfig {
    @Default
    private final String host = "localhost";

    @Default
    private final int port = 2878;

    @NonNull
    private final String apiToken;

    @Default
    private final Duration exportDuration = Duration.ofMinutes(1);
}
