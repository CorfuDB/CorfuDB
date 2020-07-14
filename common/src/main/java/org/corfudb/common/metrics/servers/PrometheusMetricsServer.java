package org.corfudb.common.metrics.servers;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.MetricsServer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

@Slf4j
public class PrometheusMetricsServer implements MetricsServer {
  private final Server server;
  private final Config config;

  public PrometheusMetricsServer(Config config, MetricRegistry metricRegistry) {
    this.config = config;
    this.server = new Server(config.getPort());
    CollectorRegistry.defaultRegistry.register(new DropwizardExports(metricRegistry));
  }

  /** Start server if enabled and not started yet. */
  @Override
  public synchronized void start() {
    if (server.isStarted() || !config.isEnabled()) {
      return;
    }

    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.setContextPath("/");
    server.setHandler(contextHandler);
    contextHandler.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
    try {
      server.start();
      log.info("setupMetrics: reporting metrics on port {}", config.getPort());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Clean up if server is running. */
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

  @AllArgsConstructor
  @Getter
  public static class Config {
    private final int port;
    private final boolean enabled;
    public static final Boolean ENABLED = true;
    public static final String METRICS_PARAM = "--metrics";
    public static final String METRICS_PORT_PARAM = "--metrics-port";

    public static Config parse(Map<String, Object> opts) {
      boolean enabled =
          Optional.ofNullable(opts.get(METRICS_PARAM)).map(e -> (boolean) e).orElse(false);
      int port =
          Optional.ofNullable(opts.get(METRICS_PORT_PARAM))
              .map(p -> Integer.parseInt(p.toString()))
              .orElse(-1);
      return new Config(port, enabled);
    }
  }
}
