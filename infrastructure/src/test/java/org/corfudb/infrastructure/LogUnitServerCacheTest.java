package org.corfudb.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider.MeterRegistryInitializer;
import org.corfudb.infrastructure.BatchProcessor.BatchProcessorContext;
import org.corfudb.infrastructure.LogUnitServer.LogUnitServerConfig;
import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;


class LogUnitServerCacheTest {

    @Test
    public void testMetrics() {
        String defaultMetricsLoggerName = LogUnitServerCacheTest.class.getName();
        Duration defaultMetricsLoggingInterval = Duration.ofMinutes(1);

        Logger logger = LoggerFactory.getLogger(defaultMetricsLoggerName);
        String endpoint = "127.0.0.1:9000";

        MeterRegistryInitializer.initServerMetrics(logger, defaultMetricsLoggingInterval, endpoint);

        LogUnitServerConfig config = LogUnitServerConfig.builder()
                .cacheSizeHeapRatio(0.5)
                .maxCacheSize(10_000_000)
                .memoryMode(true)
                .noSync(true)
                .build();
        StreamLog streamLog = new InMemoryStreamLog(new BatchProcessorContext());
        LogUnitServerCache cache = new LogUnitServerCache(config, streamLog);

        int addr = 1;
        LogData logData = generateLogData(addr);
        streamLog.append(addr, logData);

        ILogData value = cache.get(addr);
        System.out.println("yay: " + value);

        MeterRegistry meterRegistry = MeterRegistryProvider.getInstance()
                .orElseThrow(() -> new IllegalStateException("Meter registry not initialized"));


    }

    private static LogData generateLogData(long address) {
        LogData ld = new LogData(DataType.DATA);
        ld.setGlobalAddress(address);
        ld.setEpoch(1L);
        return ld;
    }

}