package org.corfudb.runtime.stream;

import lombok.SneakyThrows;
import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.infrastructure.StreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.corfudb.util.RandomOpenPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 8/29/15.
 */
public class NewStreamTest {

    CorfuInfrastructureBuilder infrastructure;
    CorfuDBRuntime runtime;
    ICorfuDBInstance instance;

    @Before
    @SneakyThrows
    public void setup()
    {
        infrastructure =
                CorfuInfrastructureBuilder.getBuilder()
                        .addSequencer(RandomOpenPort.getOpenPort(), NettyStreamingSequencerServer.class, "nsss", null)
                        .addLoggingUnit(RandomOpenPort.getOpenPort(), 0, NettyLogUnitServer.class, "nlu", null)
                        .start(RandomOpenPort.getOpenPort());

        runtime = CorfuDBRuntime.getRuntime(infrastructure.getConfigString());
        instance = runtime.getLocalInstance();
    }

  //  @Test
    public void streamReadWrite()
            throws Exception
    {
        NewStream ns = new NewStream(UUID.randomUUID(), instance);
        ns.append("Hello World");
        assertThat(ns.readNextObject())
                .isEqualTo("Hello World");
    }

    @Test
    public void AsyncReadAll()
            throws Exception
    {
        NewStream ns = new NewStream(UUID.randomUUID(), instance);
        ns.readAtAddress(10).get();
        int num = 10;
        for (int i = 0; i< num; i++) {
            ns.append("Hello World");
        }
        IStreamEntry[] es = ns.readToAsync(new SimpleTimestamp(10))
                .get();
        assertThat(es.length)
                .isEqualTo(10);
    }

   // @Test
    public void streamsAreIndependent()
            throws Exception
    {
        NewStream ns1 = new NewStream(UUID.randomUUID(), instance);
        NewStream ns2 = new NewStream(UUID.randomUUID(), instance);
        ns1.append("Hello World from stream 1");
        ns2.append("Hello World from stream 2");
        assertThat(ns1.readNextObject())
                .isEqualTo("Hello World from stream 1");
        assertThat(ns2.readNextObject())
                .isEqualTo("Hello World from stream 2");
        assertThat(ns1.readNextObject())
                .isEqualTo(null);
        assertThat(ns2.readNextObject())
                .isEqualTo(null);
    }

    @Test
    public void HoleFillingIsTransparent()
        throws Exception
    {
        NewStream ns1 = new NewStream(UUID.randomUUID(), instance);
        NewStream ns2 = new NewStream(UUID.randomUUID(), instance);
        ns2.reserve(1);
        ns1.append("Hello World from stream 1");
        ITimestamp ts = ns1.checkAsync().get();
        assertThat(ns1.readToAsync(ts).get()[1].getPayload())
                .isEqualTo("Hello World from stream 1");
    }

    @After
    @SneakyThrows
    public void tearDown()
    {
        infrastructure
                .shutdownAndWait();
    }
}
