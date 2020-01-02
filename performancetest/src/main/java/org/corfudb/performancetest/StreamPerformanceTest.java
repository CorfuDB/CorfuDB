package org.corfudb.performancetest;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;
import java.io.IOException;

/**
 * Created by Nan Zhang and Lin Dong.
 */

public class StreamPerformanceTest extends PerformanceTest {
    private static final MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();
    private final Timer producerTimer;
    private final Timer consumerTimer;
    private static final String METRIC_PREFIX = "corfu-perf.";
    private static final String STREAM_NAME = "stream-perf-it";
    private static final String OBJECT_NUM = "appendObjectNum";
    private static final String OBJECT_SIZE = "appendObjectSize";
    private static final String PRODUCER_NUM = "producerNum";

    public StreamPerformanceTest() {
        producerTimer = metricRegistry.timer(METRIC_PREFIX + "stream-producer");
        consumerTimer = metricRegistry.timer(METRIC_PREFIX + "stream-consumer");
    }

    /**
     * tests append() and next() performance on a stream,
     * with single producer and single consumer in two threads.
     *
     * This test does not check correctness.
     */
    @Test
    public void streamSingleProducer() throws IOException, InterruptedException {
        setMetricsReportFlags("stream-single");
        Process server = runServer();
        CorfuRuntime runtime = initRuntime();
        int objectNum = Integer.parseInt(PROPERTIES.getProperty(OBJECT_NUM, "256"));
        int objectSize = Integer.parseInt(PROPERTIES.getProperty(OBJECT_SIZE, "4096"));
        byte[] payload = new byte[objectSize];

        Thread p = new Thread(() -> populateStream(runtime, objectNum, payload));
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
        killServer(server);
    }

    /**
     * tests append() and next() performance on a stream,
     * with multiple producers and single consumer in different threads.
     *
     * This test does not check correctness.
     */
    @Test
    public void streamMultipleProducers() throws IOException, InterruptedException {
        setMetricsReportFlags("stream-multiple");
        Process server = runServer();
        CorfuRuntime runtime = initRuntime();
        int objectNum = Integer.parseInt(PROPERTIES.getProperty(OBJECT_NUM, "256"));
        int objectSize = Integer.parseInt(PROPERTIES.getProperty(OBJECT_SIZE, "4096"));
        int producerNum = Integer.parseInt(PROPERTIES.getProperty(PRODUCER_NUM, "3"));
        byte[] payload = new byte[objectSize];

        for (int i = 0; i < producerNum; i++) {
            Thread p = new Thread(() -> populateStream(runtime, objectNum, payload));
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
        killServer(server);
    }

    private void populateStream(CorfuRuntime runtime, int objectNum, byte[] payload) {
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID(STREAM_NAME));
        for (int i = 0; i < objectNum; i++) {
            Timer.Context context = producerTimer.time();
            sv.append(payload);
            context.stop();
        }
    }
}
