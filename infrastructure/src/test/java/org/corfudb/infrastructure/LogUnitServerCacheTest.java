package org.corfudb.infrastructure;

import io.micrometer.core.instrument.FunctionCounter;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider.MeterRegistryInitializer;
import org.corfudb.infrastructure.BatchProcessor.BatchProcessorContext;
import org.corfudb.infrastructure.LogUnitServer.LogUnitServerConfig;
import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.util.LambdaUtils.BiOptional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;


class LogUnitServerCacheTest {
    private final Random rnd = new Random();

    @Test
    public void testMetrics() {
        String defaultMetricsLoggerName = LogUnitServerCacheTest.class.getName();
        Duration defaultMetricsLoggingInterval = Duration.ofMinutes(1);

        Logger logger = LoggerFactory.getLogger(defaultMetricsLoggerName);
        String endpoint = "127.0.0.1:9000";

        MeterRegistryInitializer.initServerMetrics(logger, defaultMetricsLoggingInterval, endpoint);

        LogUnitServerConfig config = LogUnitServerConfig.builder()
                .cacheSizeHeapRatio(0.5)
                .maxCacheSize(100_000)
                .memoryMode(true)
                .noSync(true)
                .build();
        StreamLog streamLog = new InMemoryStreamLog(new BatchProcessorContext());
        LogUnitServerCache cache = new LogUnitServerCache(config, streamLog);

        for (int addr = 1; addr < 101; addr++) {
            LogData logData = generateLogData(addr);
            streamLog.append(addr, logData);
        }

        for (int i = 0; i < 500; i++) {
            cache.get(rnd.nextInt(150));
        }

        BiOptional<FunctionCounter> missAndHit = BiOptional.of(cache.miss(), cache.hit());
        if (missAndHit.isEmpty()) {
            Assertions.fail("Metrics not initialized");
        }

        missAndHit.ifPresent((miss, hit) -> {
            //compare with hit ratio
            double expected = hit.count() / (hit.count() + miss.count());
            if (cache.hitRatio().isPresent()) {
                assertEquals(expected, cache.hitRatio().get().value());
            } else {
                Assertions.fail("Hit ration not present");
            }
        });
    }

    private static LogData generateLogData(long address) {
        LogData ld = new LogData(DataType.DATA);
        ld.setGlobalAddress(address);
        ld.setEpoch(1L);
        return ld;
    }

}