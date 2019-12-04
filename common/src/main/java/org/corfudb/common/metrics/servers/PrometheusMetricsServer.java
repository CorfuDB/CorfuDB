package org.corfudb.common.metrics.servers;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import lombok.extern.slf4j.Slf4j;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import org.corfudb.common.metrics.MetricsServer;

@Slf4j
public class PrometheusMetricsServer implements MetricsServer {
    private final Server server;
    private final int port;

    public PrometheusMetricsServer(int port, MetricRegistry metricRegistry) {
        this.port = port;
        this.server = new Server(this.port);
        CollectorRegistry.defaultRegistry.register(new DropwizardExports(metricRegistry));
    }

    /**
     * Start server if enabled and not started yet.
     */
    @Override
    public synchronized void start() {
        // If config isnt enabled then why even return here :s
        if (server.isStarted()) {
            return;
        }

        ServletContextHandler contextHandler = new ServletContextHandler();
        contextHandler.setContextPath("/");
        server.setHandler(contextHandler);
        contextHandler.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        try {
            server.start();
            log.info("setupMetrics: reporting metrics on port {}", this.port);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Clean up if server is running.
     */
    @Override
    public synchronized void stop() {
        if (!server.isRunning()) {
            return;
        }

        try {
            server.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
