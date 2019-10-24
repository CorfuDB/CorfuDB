package org.corfudb.performancetest;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;
import java.util.Properties;

/**
 * Created by Nan Zhang and Lin Dong on 10/23/19.
 */

public class StreamPerformanceTest {
    private String endPoint = "localhost:9000";
    private int metricsPort = 1000;
    private static final Properties PROPERTIES = new Properties();
    static MetricRegistry metricRegistry;
    private static final String METRIC_PREFIX = "corfu-perf";
    private static final String STREAM_NAME = "stream-perf-it";
    private static final String OBJECT_NUM = "appendObjectNum";
    private static final String OBJECT_SIZE = "appendObjectSize";
    private static final String PRODUCER_NUM = "producerNum";
    CorfuRuntime runtime;
    Timer producerTimer;
    Timer consumerTimer;

    public StreamPerformanceTest() {
        runtime = initRuntime();
        metricRegistry = CorfuRuntime.getDefaultMetrics();
        producerTimer = metricRegistry.timer(METRIC_PREFIX + "stream-single-producer");
        consumerTimer = metricRegistry.timer(METRIC_PREFIX + "stream-single-consumer");
    }
    private CorfuRuntime initRuntime() {
        CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
        parameters.setPrometheusMetricsPort(metricsPort);
        CorfuRuntime corfuRuntime = CorfuRuntime.fromParameters(parameters);
        corfuRuntime.addLayoutServer(endPoint);
        corfuRuntime.connect();
        return corfuRuntime;
    }

    /**
     * tests append() and next() performance on a stream,
     * with single producer and single consumer in two threads.
     *
     * This test does not check correctness.
     */
    @Test
    public void streamSingleProducer() {
        int objectNum = Integer.parseInt(PROPERTIES.getProperty(OBJECT_NUM, "256"));
        int objectSize = Integer.parseInt(PROPERTIES.getProperty(OBJECT_SIZE, "4096"));
        byte[] payload = new byte[objectSize];

        Thread p = new Thread(() -> populateStream(objectNum, payload, producerTimer));
        p.start();

        int counter = 0;
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID(STREAM_NAME));
        while (counter < objectNum) {
            if (sv.hasNext()) {
                Timer.Context context = consumerTimer.time();
                sv.next();
                context.stop();
                counter++;
            }
        }
    }

    /**
     * tests append() and next() performance on a stream,
     * with multiple producers and single consumer in different threads.
     *
     * This test does not check correctness.
     */
    @Test
    public void streamMultipleProducers() {
        int objectNum = Integer.parseInt(PROPERTIES.getProperty(OBJECT_NUM, "256"));
        int objectSize = Integer.parseInt(PROPERTIES.getProperty(OBJECT_SIZE, "4096"));
        int producerNum = Integer.parseInt(PROPERTIES.getProperty(PRODUCER_NUM, "3"));
        byte[] payload = new byte[objectSize];

        for (int i = 0; i < producerNum; i++) {
            Thread p = new Thread(() -> populateStream(objectNum, payload, producerTimer));
            p.start();
        }

        int counter = 0;
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID(STREAM_NAME));
        while (counter < objectNum * producerNum) {
            if (sv.hasNext()) {
                Timer.Context context = consumerTimer.time();
                sv.next();
                context.stop();
                counter++;
            }
        }
    }

    private void populateStream(int objectNum, byte[] payload, Timer producerTimer) {
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID(STREAM_NAME));
        for (int i = 0; i < objectNum; i++) {
            Timer.Context context = producerTimer.time();
            sv.append(payload);
            context.stop();
        }
    }
}
