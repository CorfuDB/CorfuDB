package org.corfudb.common.metrics.micrometer;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

public class MeterRegistryProvider {
    private static Optional<MeterRegistry> meterRegistry;

    private MeterRegistryProvider() {

    }

    public static Optional<MeterRegistry> getInstance() {
        return meterRegistry;
    }

    public static Optional<MeterRegistry> createLoggingMeterRegistry(Logger logger, Duration loggingInterval) {
        Supplier<Optional<MeterRegistry>> supplier = () -> {
            LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
            return Optional.of(LoggingMeterRegistry.builder(config)
                    .loggingSink(logger::info).build());
        };

        return create(supplier);
    }

    public static Optional<MeterRegistry> createLoggingMeterRegistry(Logger logger) {
        return create(() -> createLoggingMeterRegistry(logger, Duration.ofMinutes(1)));
    }

    public static Optional<MeterRegistry> createLoggingMeterRegistry() {
        return create(() -> Optional.of(new LoggingMeterRegistry()));
    }

    public static Optional<MeterRegistry> createPrometheusMeterRegistry(int exporterPort, String path) {
        Supplier<Optional<MeterRegistry>> supplier = () -> {
            PrometheusMeterRegistry prometheusMeterRegistry =
                    new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            try {
                HttpServer server = HttpServer.create(new InetSocketAddress(exporterPort), 0);
                server.createContext("/" + path, httpExchange -> {
                    String response = prometheusMeterRegistry.scrape();
                    httpExchange.sendResponseHeaders(200, response.getBytes().length);
                    try (OutputStream os = httpExchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                });

                new Thread(server::start).start();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return Optional.of(prometheusMeterRegistry);
        };

        return create(supplier);
    }

    private static Optional<MeterRegistry> create(Supplier<Optional<MeterRegistry>> meterRegistrySupplier) {
        if (!meterRegistry.isPresent()) {
            synchronized (MeterRegistryProvider.class) {
                if (!meterRegistry.isPresent()) {
                    meterRegistry = meterRegistrySupplier.get();
                }
            }
        }
        return meterRegistry;
    }
}
