package org.corfudb.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Gauge;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.test.CorfuServerRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is an integration test that exercises the functionality and expectations of
 * memory measurement tool provided in {@link MetricsUtils}.
 *
 * Created by Sam Behnam on 7/16/18.
 */
@Slf4j
public class MemoryFootprintMeasurerIT extends AbstractIT{
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
     * This test measures the size of an object (an instance of an ArrayList)
     * while adding small and large items to the list and then remove them. It
     * evaluates that measured size of list follows the expectation. Note that
     * evaluating the removal will depend on the request to System for garbage
     * collection taking effect. This is not deterministic and depends on JVM's
     * priorities.
     *
     * @throws Exception
     */
    @Test
    public void testMemoryFootprintMeasurerUsingArrayList() throws Exception {
        final int smallSize = 10;
        final int largeSize = 10000;

        // Run a corfu server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
                                                        corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        // Create an object be measured
        List<byte[]> arrayList = new ArrayList<>();

        // Create memory measurer for the object under investigation
        final Gauge<Long> arrayListSizeGauge =
                MetricsUtils.addMemoryMeasurerFor(CorfuRuntime.getDefaultMetrics(), arrayList);
        final Long initialArraySize = arrayListSizeGauge.getValue();

        // Increase the memory consumption and assert the expected behavior
        arrayList.add(new byte[smallSize]);
        final Long arraySizeAfterSmallIncrease = arrayListSizeGauge.getValue();
        log.info("ArrayList size:{}", arraySizeAfterSmallIncrease);
        assertThat(arraySizeAfterSmallIncrease - initialArraySize)
                .isGreaterThanOrEqualTo(smallSize);

        arrayList.add(new byte[largeSize]);
        final Long arraySizeAfterLargeIncrease = arrayListSizeGauge.getValue();
        log.info("ArrayList size:{}", arraySizeAfterLargeIncrease);
        assertThat(arraySizeAfterLargeIncrease - arraySizeAfterSmallIncrease)
                .isGreaterThanOrEqualTo(largeSize);

        // Decrease the memory consumption and assert the expected behavior
        arrayList.clear();
        System.gc();
        final Long arraySizeAfterRemoval = arrayListSizeGauge.getValue();
        log.info("ArrayList size:{}", arraySizeAfterRemoval);
        assertThat(arraySizeAfterRemoval).isLessThan(arraySizeAfterLargeIncrease);

        // Assert the server is shutdown
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test examines whether {@link MetricsUtils} follows the contract of not
     * retaining a strong reference to objects that are being measured. As a result,
     * once the test removes (null out) a reference, it should be reflected on the
     * size returned by the gauge for that object.
     * Note that evaluating the removal will depend on the request to System for
     * garbage collection taking effect. This is not deterministic and depends on
     * JVM's priorities.
     *
     * @throws Exception
     */
    @Test
    public void testWeakReferenceRemoval() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
                                                        corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        // Create and fill a map that will be used to assess nulling out behavior
        Map<String, String> map1 = new HashMap<>();
        final Gauge<Long> map1Measurer =
                MetricsUtils.addMemoryMeasurerFor(CorfuRuntime.getDefaultMetrics(), map1);

        final int countMap1 = 300000;
        for (int i = 0; i < countMap1; i++) {
            map1.put("t1-key-" + i, "t1-value-" + i);
        }

        log.info("Size of map1 before removing reference: {}",
                map1Measurer.getValue());

        // Null out the variable under investigation and request a garbage collection
        map1 = null;
        System.gc();

        // Create and fill a map that will not be nulled out
        Map<String, String> map2 = new HashMap<>();
        final Gauge<Long> map2Measurer =
                MetricsUtils.addMemoryMeasurerFor(CorfuRuntime.getDefaultMetrics(), map2);

        final int countMap2 = 1000;
        for (int i = 0; i < countMap2; i++) {
            map2.put("t2-key-" + i, "t2-value" + i);
        }

        // Assert the that once the reference to an object is removed, MetricUtil
        // does not hold a reference to the object.
        assertThat(map1Measurer.getValue()).isEqualTo(0);
        assertThat(map2Measurer.getValue()).isNotEqualTo(0);
        log.info("Size of map1 after nulling out the reference:{}",
                map1Measurer.getValue());
        log.info("Size of map2 after at the end of test:{}",
                map2Measurer.getValue());

        // Assert the server is shutdown
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }
}
