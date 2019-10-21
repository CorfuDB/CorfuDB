package org.corfudb.perf;

import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;
import org.corfudb.util.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@Slf4j
public class LogUnitPerfTest extends AbstractPerfTest {
    private static final String READ_PERCENT = "logunitReadPercent";
    private static final String SINGLE_REQUEST = "logunitSingleRequests";
    private static final String BATCH_REQUEST = "logunitBatchRequests";
    private static final String BATCH_SIZE = "logunitBatchSize";

    private CorfuRuntime corfuRuntime;
    private LogUnitClient client;
    private String endpoint;
    private long epoch;

    @Before
    public void setUp() {
        String corfuHost = properties.getProperty(CORFU_CLUSTER_HOST, "localhost");
        String corfuPort = properties.getProperty(CORFU_CLUSTER_PORT, "9000");
        String configuration = String.format("%s:%s", corfuHost, corfuPort);
        endpoint = configuration;
        corfuRuntime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                .build())
                .parseConfigurationString(configuration)
                .connect();
        epoch = corfuRuntime.getLayoutView().getCurrentLayout().getEpoch();
        System.out.println("Current epoch is " + epoch);
        client = new LogUnitClient(corfuRuntime.getRouter(configuration), epoch);
    }

    @After
    public void tearDown() {
        // need to reset cluster to avoid overwrite exception
        // however, each epoch only can be reset once, hence update epoch
        client.resetLogUnit(epoch);
        Layout currentLayout = new Layout(corfuRuntime.getLayoutView().getCurrentLayout());
        currentLayout.setEpoch(currentLayout.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(currentLayout).sealMinServerSet();
        corfuRuntime.shutdown();
        // wait for updating new layout at higher epoch
        Sleep.sleepUninterruptibly(Duration.ofSeconds(10));
    }

    @Test
    public void logunitReadWrite() throws ExecutionException, InterruptedException {
        int numRequests = Integer.parseInt(properties.getProperty(SINGLE_REQUEST, "100"));
        int readPercent = Integer.parseInt(properties.getProperty(READ_PERCENT, "50"));
        String randomSeed = properties.getProperty(RANDOM_SEED, null);
        Random random = randomSeed == null ? new Random() : new Random(Long.parseLong(randomSeed));

        Timer readTimer = metricRegistry.timer(METRIC_PREFIX + "logunit-single-read");
        Timer writeTimer = metricRegistry.timer(METRIC_PREFIX + "logunit-single-write");

        client = new LogUnitClient(corfuRuntime.getRouter(endpoint), epoch);
        byte[] testString = "Perf Test Payload".getBytes();
        int address = 0;
        for (int i = 0; i < numRequests; i++) {
            if (writeTimer.getCount() == 0 || random.nextInt(RANDOM_UPPER_BOUND) > readPercent) {
                Timer.Context context = writeTimer.time();
                client.write(address++, null, testString, Collections.emptyMap()).get();
                context.stop();
            } else {
                int pos = random.nextInt(address);
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
    public void logunitBatchReadWrite() throws ExecutionException, InterruptedException {
        int numRequests = Integer.parseInt(properties.getProperty(BATCH_REQUEST, "50"));
        int batchSize = Integer.parseInt(properties.getProperty(BATCH_SIZE, "15"));
        int readPercent = Integer.parseInt(properties.getProperty(READ_PERCENT, "50"));
        String randomSeed = properties.getProperty(RANDOM_SEED, null);
        Random random = randomSeed == null ? new Random() : new Random(Long.parseLong(randomSeed));

        Timer readTimer = metricRegistry.timer(METRIC_PREFIX + "logunit-batch-read");
        Timer writeTimer = metricRegistry.timer(METRIC_PREFIX + "logunit-batch-write");

        client = new LogUnitClient(corfuRuntime.getRouter(endpoint), epoch);
        byte[] streamEntry = "Perf Test Payload".getBytes();
        int address = 0;
        for (int i = 0; i < numRequests; i++) {
            if (writeTimer.getCount() == 0 || random.nextInt(RANDOM_UPPER_BOUND) > readPercent) {
                List<LogData> entries = new ArrayList<>();
                for (int x = 0; x < batchSize; x++) {
                    ByteBuf b = Unpooled.buffer();
                    Serializers.CORFU.serialize(streamEntry, b);
                    LogData ld = new LogData(DataType.DATA, b);
                    ld.setGlobalAddress((long)address++);
                    entries.add(ld);
                }
                Timer.Context context = writeTimer.time();
                client.writeRange(entries).get();
                context.stop();
            } else {
                long start = random.nextInt((int) writeTimer.getCount()) * batchSize;
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
