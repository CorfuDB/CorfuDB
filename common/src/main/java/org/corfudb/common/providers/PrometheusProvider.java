package org.corfudb.common.providers;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import lombok.extern.slf4j.Slf4j;


import io.prometheus.client.exporter.MetricsServlet;

import org.corfudb.common.Provider;
import org.corfudb.common.StatsLogger;
import org.corfudb.common.loggers.DropWizardLogger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

@Slf4j
public class PrometheusProvider implements Provider {
    private final StatsLogger rootLogger;
    private final Server server;
    public PrometheusProvider(int port, StatsLogger logger) {
        this.rootLogger = logger;
        this.server = new Server(port);
        MetricRegistry metricRegistry = ((DropWizardLogger)this.rootLogger).getMetricRegistry();
        //MetricRegistry metricRegistry = new MetricRegistry();
        //metricRegistry.counter("test-in-provider").inc();
        CollectorRegistry.defaultRegistry.register(new DropwizardExports(metricRegistry));
    }

    @Override
    public StatsLogger getLogger(String name) {
        return rootLogger.scope(name);
    }

    @Override
    public synchronized void start() {
        if (!server.isStarted()) {
            ServletContextHandler contextHandler = new ServletContextHandler();
            contextHandler.setContextPath("/");
            server.setHandler(contextHandler);
            contextHandler.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
            try {
                server.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public synchronized void stop() {
        if (server.isRunning()) {
            try {
                server.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
