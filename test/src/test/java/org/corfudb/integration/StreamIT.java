package org.corfudb.integration;

import org.HdrHistogram.Histogram;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT  {

    static final long a = 3600000000000L;
    static final int b = 3;
    static final double c = 1.0;
    static Histogram histogram = new Histogram(a, b);

    @Test
    public void simpleStreamTest() throws Exception {

        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();

/*
        ManagementClient managementClient = rt.getRouter("localhost:9000").getClient(ManagementClient.class);

        managementClient.addNodeRequest("localhost:9003").get();

        System.out.println("Done");

*/
        //Map<String, String> map = rt.getObjectsView().build().setType(SMRMap.class).setStreamName("map2").open();

        final int iter = 30000 * 4;
        final int payload = 4000 * 2;
        byte[] data = new byte[payload];
        for (int x = 0; x < iter; x++) {
            long startTime = System.currentTimeMillis();
            rt.getStreamsView().get(CorfuRuntime.getStreamID("s1")).append(data);
            long stopTime = System.currentTimeMillis();
            histogram.recordValue(stopTime - startTime);
        }

        histogram.outputPercentileDistribution(System.out, c);

    }
}
