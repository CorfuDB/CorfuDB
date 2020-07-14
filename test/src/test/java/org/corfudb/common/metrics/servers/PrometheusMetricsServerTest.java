package org.corfudb.common.metrics.servers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PrometheusMetricsServerTest {
  @Test
  void testConfigParseEmptyOpts() {
    Map<String, Object> map = new HashMap<>();
    PrometheusMetricsServer.Config config = PrometheusMetricsServer.Config.parse(map);
    assertFalse(config.isEnabled());
  }

  @Test
  void testConfigParseEnabled() {
    Map<String, Object> map = new HashMap<>();
    map.put(PrometheusMetricsServer.Config.METRICS_PARAM, true);
    PrometheusMetricsServer.Config config = PrometheusMetricsServer.Config.parse(map);
    assertTrue(config.isEnabled());
  }
}
