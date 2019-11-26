package org.corfudb.performancetest;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import java.util.*;

/**
 * Created by Nan Zhang and Lin Dong on 10/23/19.
 */

public class LogUnitPerformanceTest extends PerformanceTest{
    static MetricRegistry metricRegistry;
    Timer readTimer;
    Timer writeTimer;
    private static final int randomBoundary = 100;
    private static final String METRIC_PREFIX = "corfu-perf.";
    private static final String READ_PERCENT = "logunitReadPercent";
    private static final String SINGLE_REQUEST = "logunitSingleRequests";
    private static final String BATCH_REQUEST = "logunitBatchRequests";
    private static final String BATCH_SIZE = "logunitBatchSize";
    private static final String ENTRY_SIZE = "logEntrySize";

    public LogUnitPerformanceTest() {
        metricRegistry = CorfuRuntime.getDefaultMetrics();
        readTimer = metricRegistry.timer(METRIC_PREFIX + "logunit-read");
        writeTimer = metricRegistry.timer(METRIC_PREFIX + "logunit-write");
    }

    /**
     * tests write() and read() performance of logunit client,
     * use random to generate and send mixed requests.
     *
     * This test does not check correctness.
     */
    @Test
    public void logunitSingleReadWrite() throws Exception {
        setMetricsReportFlags("logunit-single");
        Process server = runServer();
        CorfuRuntime runtime = initRuntime();
        int numRequests = Integer.parseInt(PROPERTIES.getProperty(SINGLE_REQUEST, "100"));
        int readPercent = Integer.parseInt(PROPERTIES.getProperty(READ_PERCENT, "50"));
        int entrySize = Integer.parseInt(PROPERTIES.getProperty(ENTRY_SIZE, "2048"));

        LogUnitClient client = new LogUnitClient(runtime.getRouter(endPoint), 0L);
        int address = 0;
        byte[] payload = new byte[entrySize];

        for (int i = 0; i < numRequests; i++) {
            if (address == 0 || random.nextInt(randomBoundary) > readPercent) {
                Timer.Context context = writeTimer.time();
                client.write(address++, null, payload, Collections.emptyMap()).get();
                context.stop();
            } else {
                int pos = random.nextInt(address);
                Timer.Context context = readTimer.time();
                client.read(pos).get();
                context.stop();
            }
        }
        killServer(server);
    }

    /**
     * tests writeRange() and readAll() performance of logunit client,
     * use random to generate and send mixed requests.
     *
     * This test does not check correctness.
     */
    @Test
    public void logunitRangeReadWrite() throws Exception {
        setMetricsReportFlags("logunit-range");
        Process server = runServer();
        CorfuRuntime runtime = initRuntime();
        int numRequests = Integer.parseInt(PROPERTIES.getProperty(BATCH_REQUEST, "50"));
        int batchSize = Integer.parseInt(PROPERTIES.getProperty(BATCH_SIZE, "15"));
        int readPercent = Integer.parseInt(PROPERTIES.getProperty(READ_PERCENT, "50"));
        int entrySize = Integer.parseInt(PROPERTIES.getProperty(ENTRY_SIZE, "2048"));

        LogUnitClient client = new LogUnitClient(runtime.getRouter(endPoint), 0L);
        byte[] payload = new byte[entrySize];
        int address = 0;
        int writeCount = 0;
        for (int i = 0; i < numRequests; i++) {
            if (address == 0 || random.nextInt(randomBoundary) > readPercent) {
                List<LogData> entries = new ArrayList<>();
                for (int x = 0; x < batchSize; x++) {
                    ByteBuf b = Unpooled.buffer();
                    Serializers.CORFU.serialize(payload, b);
                    LogData ld = new LogData(DataType.DATA, b);
                    ld.setGlobalAddress((long)address++);
                    entries.add(ld);
                }
                writeCount++;
                Timer.Context context = writeTimer.time();
                client.writeRange(entries).get();
                context.stop();

            } else {
                long start = random.nextInt(writeCount) * batchSize;
                List<Long> addresses = new ArrayList<>();
                for (long x = start; x < batchSize; x++) {
                    addresses.add(x);
                }
                Timer.Context context = readTimer.time();
                client.readAll(addresses).get();
                context.stop();
            }
        }
        killServer(server);
    }
}
