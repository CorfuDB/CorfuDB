package org.corfudb.runtime.stream;

import org.corfudb.infrastructure.NewLogUnitServer;
import org.corfudb.infrastructure.StreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.CorfuInfrastructureBuilder;
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
    public void setup()
    {
        infrastructure =
                CorfuInfrastructureBuilder.getBuilder()
                        .addSequencer(7776, StreamingSequencerServer.class, "cdbsts", null)
                        .addLoggingUnit(7777, 0, NewLogUnitServer.class, "cnlu", null)
                        .start(7775);

        runtime = CorfuDBRuntime.getRuntime(infrastructure.getConfigString());
        instance = runtime.getLocalInstance();
    }

    @Test
    public void streamReadWrite()
            throws Exception
    {
        NewStream ns = new NewStream(UUID.randomUUID(), instance);
        ns.append("Hello World");
        assertThat(ns.readNextObject())
                .isEqualTo("Hello World");
    }

    @Test
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
        assertThat(ns1.readNextObject())
                .isEqualTo("Hello World from stream 1");
    }

    @After
    public void tearDown()
    {
        infrastructure
                .shutdownAndWait();
    }
}
