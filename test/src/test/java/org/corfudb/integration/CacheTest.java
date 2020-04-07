package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.common.compression.Codec;
import org.corfudb.runtime.CorfuRuntime;

import java.io.IOException;
import java.util.Map;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CacheTest extends AbstractIT {
    private static final int iterations = 100;
    private static final int numTables = 10;
    private static final int payloadSize = 2;

    @Test
    public void cacheTest() throws IOException, InterruptedException {

        Process server1 = new AbstractIT.CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .setLogPath("/tmp")
                .runServer();
        final int maxWriteSize = 20000;
        Map<String, byte[]>[] maps = new Map[numTables];
        Map<String, byte[]>[] maps_new = new Map[numTables];

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .maxWriteSize(maxWriteSize)
                .codecType(Codec.Type.NONE)
                .build();

        runtime = CorfuRuntime.fromParameters(params);
        runtime.parseConfigurationString(DEFAULT_ENDPOINT);
        runtime.connect();

        for(int i=0; i<numTables; i++) {
            maps[i] = runtime.getObjectsView()
                    .build()
                    .setTypeToken(new TypeToken<CorfuTable<String, byte[]>>() {})
                    .setStreamName("table" + i)
                    .serializer(Serializers.JAVA)
                    .open();
        }

        byte[] smallPayload = new byte[payloadSize];

        long start = System.currentTimeMillis();

        for(int i=0; i<iterations; i++) {
            runtime.getObjectsView().TXBegin();
            for(int j=0; j<numTables; j++) {
                maps[j].put(Integer.toString(i), smallPayload);
            }
            runtime.getObjectsView().TXEnd();
        }


        long end = System.currentTimeMillis();
        System.out.println("Total Write Time - " + (end - start));

        // Verify that all those changes can be observed from a new client
        CorfuRuntime rt1 = CorfuRuntime.fromParameters(params);
        rt1.parseConfigurationString(DEFAULT_ENDPOINT);
        rt1.connect();
        for(int i=0; i<numTables; i++) {
            maps_new[i] = runtime.getObjectsView()
                    .build()
                    .setTypeToken(new TypeToken<CorfuTable<String, byte[]>>() {})
                    .setStreamName("table" + i)
                    .serializer(Serializers.JAVA)
                    .open();
        }

        start = System.currentTimeMillis();
        for(int i=0; i<iterations; i++) {
            for(int j=0; j<numTables; j++) {
                assertThat(maps_new[j].get(Integer.toString(i)).equals(smallPayload));
            }
        }

        end = System.currentTimeMillis();
        System.out.println("Total Read Time -" + (end - start));

        shutdownCorfuServer(server1);
        //Thread.sleep(3*60*1000);
    }
}

