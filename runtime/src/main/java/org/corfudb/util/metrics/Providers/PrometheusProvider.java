package org.corfudb.util.metrics.Providers;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.util.metrics.MetricsProvider;
import org.corfudb.util.metrics.StatsLogger;
import org.corfudb.util.metrics.loggers.CodeHaleLogger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Slf4j
public class PrometheusProvider implements MetricsProvider {

    ThreadFactory providerThreadFactory =
            new ThreadFactoryBuilder()
                    .setNameFormat("PrometheusProvider-%d")
                    .setDaemon(true)
                    .build();

    ExecutorService providerExecutor = Executors.newSingleThreadExecutor(providerThreadFactory);

    final int port;
    final StatsLogger rootLogger;

    public PrometheusProvider(int port, StatsLogger statsLogger) {
        this.port = port;
        this.rootLogger = statsLogger;
        CollectorRegistry.defaultRegistry.register(new DropwizardExports(((CodeHaleLogger)rootLogger).getRegistry()));
    }

    @Override
    public StatsLogger getLogger(String name) {
        return rootLogger.scope(name);
    }

    private boolean started = false;

    @Override
    public synchronized void start() {
        if (!started) {
            providerExecutor.submit(() -> {
                Server server = new Server(this.port);
                ServletContextHandler context = new ServletContextHandler();
                context.setContextPath("/");
                server.setHandler(context);
                context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");

                try {
                    //TODO(Maithem): start() seems to spawn another thread, is the executor necessary?
                    server.start();
                    server.join();
                } catch (Exception e) {
                    log.error("exception in exporter ", e);
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public void stop() {
        providerExecutor.shutdownNow();
    }
}
