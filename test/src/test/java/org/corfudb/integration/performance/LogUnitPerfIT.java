package org.corfudb.integration.performance;

import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@Slf4j
public class LogUnitPerfIT extends AbstractPerfIT {
    private static final String READ_PERCENT = "logunitReadPercent";
    private static final String SINGLE_REQUEST = "logunitSingleRequests";
    private static final String BATCH_REQUEST = "logunitBatchRequests";
    private static final String BATCH_SIZE = "logunitBatchSize";
    private static final String ENTRY_SIZE = "logEntrySize";

    @Test
    public void logunitSingleReadWrite() throws Exception {
        Process server = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        runtime = CorfuRuntime.fromParameters(params);
        runtime.parseConfigurationString(DEFAULT_ENDPOINT);
        runtime.connect();

        int numRequests = Integer.parseInt(PERF_PROPERTIES.getProperty(SINGLE_REQUEST, "100"));
        int readPercent = Integer.parseInt(PERF_PROPERTIES.getProperty(READ_PERCENT, "50"));
        int entrySize = Integer.parseInt(PERF_PROPERTIES.getProperty(ENTRY_SIZE, "2048"));
        Random rand = new Random(PARAMETERS.SEED);

        Timer readTimer = metricRegistry.timer(METRIC_PREFIX + "logunit-single-read");
        Timer writeTimer = metricRegistry.timer(METRIC_PREFIX + "logunit-single-write");

        LogUnitClient client = new LogUnitClient(runtime.getRouter(DEFAULT_ENDPOINT), 0L);
        int address = 0;
        byte[] payload = new byte[entrySize];

        for (int i = 0; i < numRequests; i++) {
            if (writeTimer.getCount() == 0 || rand.nextInt(RANDOM_UPPER_BOUND) > readPercent) {
                Timer.Context context = writeTimer.time();
                client.write(address++, null, payload, Collections.emptyMap()).get();
                context.stop();
            } else {
                int pos = rand.nextInt(address);
                Timer.Context context = readTimer.time();
                client.read(pos).get();
                context.stop();
            }
        }

        // how long it will run? if short, how to report a snapshot by csv.
        log.info("logunit-single-read-timer snapshot timer is " + readTimer.getSnapshot().get98thPercentile());
        log.info("logunit-single-write-timer snapshot timer is " + writeTimer.getSnapshot().get98thPercentile());
    }

    @Test
    public void logunitRangeReadWrite() throws Exception {
        Process server = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        runtime = CorfuRuntime.fromParameters(params);
        runtime.parseConfigurationString(DEFAULT_ENDPOINT);
        runtime.connect();

        int numRequests = Integer.parseInt(PERF_PROPERTIES.getProperty(BATCH_REQUEST, "50"));
        int batchSize = Integer.parseInt(PERF_PROPERTIES.getProperty(BATCH_SIZE, "15"));
        int readPercent = Integer.parseInt(PERF_PROPERTIES.getProperty(READ_PERCENT, "50"));
        int entrySize = Integer.parseInt(PERF_PROPERTIES.getProperty(ENTRY_SIZE, "2048"));
        Random rand = new Random(PARAMETERS.SEED);

        Timer readTimer = metricRegistry.timer(METRIC_PREFIX + "logunit-batch-read");
        Timer writeTimer = metricRegistry.timer(METRIC_PREFIX + "logunit-batch-write");

        LogUnitClient client = new LogUnitClient(runtime.getRouter(DEFAULT_ENDPOINT), 0L);
        byte[] payload = new byte[entrySize];
        int address = 0;
        for (int i = 0; i < numRequests; i++) {
            if (writeTimer.getCount() == 0 || rand.nextInt(RANDOM_UPPER_BOUND) > readPercent) {
                List<LogData> entries = new ArrayList<>();
                for (int x = 0; x < batchSize; x++) {
                    ByteBuf b = Unpooled.buffer();
                    Serializers.CORFU.serialize(payload, b);
                    LogData ld = new LogData(DataType.DATA, b);
                    ld.setGlobalAddress((long)address++);
                    entries.add(ld);
                }
                Timer.Context context = writeTimer.time();
                client.writeRange(entries).get();
                context.stop();
            } else {
                long start = rand.nextInt((int) writeTimer.getCount()) * batchSize;
                List<Long> addresses = new ArrayList<>();
                for (long x = start; x < batchSize; x++) {
                    addresses.add(x);
                }
                Timer.Context context = readTimer.time();
                client.readAll(addresses).get();
                context.stop();
            }
        }
    }
}
