package org.corfudb.integration.performance;

import com.codahale.metrics.Timer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

public class StreamPerfIT extends AbstractPerfIT {
    private static final String STREAM_NAME = "stream-perf-it";
    private static final String OBJECT_NUM = "appendObjectNum";
    private static final String OBJECT_SIZE = "appendObjectSize";
    private static final String PRODUCER_NUM = "producerNum";

    /**
     * tests append() and next() performance on a stream,
     * with single producer and single consumer in two threads.
     *
     * This test does not check correctness.
     */
    @Test
    public void streamSingleProducer() throws Exception{
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

        int objectNum = Integer.parseInt(PERF_PROPERTIES.getProperty(OBJECT_NUM, "256"));
        int objectSize = Integer.parseInt(PERF_PROPERTIES.getProperty(OBJECT_SIZE, "4096"));
        byte[] payload = new byte[objectSize];


        Timer producerTimer = metricRegistry.timer(METRIC_PREFIX + "stream-single-producer");
        Timer consumerTimer = metricRegistry.timer(METRIC_PREFIX + "stream-single-consumer");

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
    public void streamMultipleProducers() throws Exception{
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

        int objectNum = Integer.parseInt(PERF_PROPERTIES.getProperty(OBJECT_NUM, "256"));
        int objectSize = Integer.parseInt(PERF_PROPERTIES.getProperty(OBJECT_SIZE, "4096"));
        int producerNum = Integer.parseInt(PERF_PROPERTIES.getProperty(PRODUCER_NUM, "3"));
        byte[] payload = new byte[objectSize];

        Timer producerTimer = metricRegistry.timer(METRIC_PREFIX + "stream-multiple-producer");
        Timer consumerTimer = metricRegistry.timer(METRIC_PREFIX + "stream-multiple-consumer");

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
