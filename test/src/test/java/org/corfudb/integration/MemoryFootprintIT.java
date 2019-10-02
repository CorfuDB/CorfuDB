package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Gauge;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.test.CorfuServerRunner;
import org.corfudb.util.MetricsUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * An integration test to utilize memory assessment utilities. It can be used as an example
 * for implementing programmatic assessment of memory footprint for objects of interest.
 * Follow the instruction in {@link MetricsUtils} in order to enable collection and reporting
 * of memory footprint metrics on objects of interest.
 *
 * Created by Sam Behnam on 6/15/18.
 */
@Slf4j
public class MemoryFootprintIT extends AbstractIT {

    private static String corfuSingleNodeHost;
    private static int corfuStringNodePort;
    private static String singleNodeEndpoint;

    /* A helper method that takes host and port specification, start a single server and
     *  returns a process. */
    private Process runSinglePersistentServer(String host, int port) throws IOException {
        return new CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(CorfuServerRunner.getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();
    }

    /** Load properties for a single node corfu server before each test*/
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    /**
     * This test utilizes the {@link MetricsUtils}'s memory measurement tools to test the memory
     * footprint assessment of the provided tool. This is done by putting key and value pairs
     * to a corfu table and then evaluating that this has the expected effects on the memory
     * consumption of the object being measured (i.e. the underlying corfu table)
     *
     * @throws Exception
     */
    @Test
    public void testCorfuTablePutMemoryFootprint() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        // Create CorfuTable
        CorfuTable testTable = runtime
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, Object>>() {})
                .setStreamName("volbeat")
                .open();

        // Force GC first to prevent it from interfering with the size estimates after.
        System.gc();

        // Register memory footprint tracking
        final Gauge<Long> corfuTableSizeGauge = MetricsUtils.addMemoryMeasurerFor(
                CorfuRuntime.getDefaultMetrics(),
                testTable);
        final Long initialSize = corfuTableSizeGauge.getValue();

        // Put key values in CorfuTable
        final int count = 100;
        final int entrySize = 100000;
        for (int i = 0; i < count; i++) {
            testTable.put(String.valueOf(i), new byte[entrySize]);
        }

        // Assert that memory measurer detects the effect of puts on table size
        final Long sizeAfterPuts = corfuTableSizeGauge.getValue();
        assertThat(sizeAfterPuts - initialSize)
                .isGreaterThanOrEqualTo(count * entrySize);
        log.info("initialSize:{}, sizeAfterPuts:{}",
                initialSize,
                sizeAfterPuts);

        // Assert that table has correct size (i.e. count) and and server is shutdown
        assertThat(testTable.size()).isEqualTo(count);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test utilizes the {@link MetricsUtils}'s memory measurement tools to test the memory
     * footprint assessment of the provided tool. This is done by removing key and value pairs
     * on a corfu table and then evaluating that this has the expected effects on the memory
     * consumption of the object being measured (i.e. the underlying corfu table)
     *
     * @throws Exception
     */
    @Test
    public void testCorfuTableRemoveMemoryFootprint() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        // Create CorfuTable
        CorfuTable testTable = runtime
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, Object>>() {})
                .setStreamName("volbeat")
                .open();

        // Register memory footprint tracking
        final Gauge<Long> corfuTableSizeGauge =
                MetricsUtils.addMemoryMeasurerFor(CorfuRuntime.getDefaultMetrics(), testTable);

        // Put key values in CorfuTable and capture the memory consumption after puts
        final int count = 100;
        final int entrySize = 100000;
        for (int i = 0; i < count; i++) {
            testTable.put(String.valueOf(i), new byte[entrySize]);
        }

        final Long sizeAfterPuts = corfuTableSizeGauge.getValue();

        // Remove (count - 1) of the added entries
        for (int i = 0; i < count - 1; i++) {
            final String keyToRemove = String.valueOf(i);
            testTable.remove(keyToRemove);
        }

        // Assert that memory measurer detects the effect of removes on table size
        System.gc();
        final Long sizeAfterRemoves = corfuTableSizeGauge.getValue();
        assertThat(sizeAfterRemoves).isLessThanOrEqualTo(sizeAfterPuts);
        log.info("sizeAfterPuts:{}, sizeAfterRemoves:{}",
                sizeAfterPuts,
                sizeAfterRemoves);

        // Assert that table has correct size (i.e. one) and and server is shutdown
        assertThat(testTable.size()).isEqualTo(1);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }
}
